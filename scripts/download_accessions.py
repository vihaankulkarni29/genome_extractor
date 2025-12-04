#!/usr/bin/env python3
"""
Download genomes from accession list with genome type filtering
"""

import os
import sys
import argparse
import csv
import time
from typing import List, Dict, Any
import sys
import os
# Add paths for imports
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, 'src'))
sys.path.insert(0, os.path.join(project_root, 'clients'))

from src.clients.ncbi_client import NCBIClient

def check_harvester_service(base_url: str = "http://127.0.0.1:8000", timeout: float = 5.0) -> bool:
    """Check if the metadata harvester service is running and accessible.
    
    Args:
        base_url: Base URL of the harvester service
        timeout: Connection timeout in seconds
        
    Returns:
        True if service is reachable, False otherwise
    """
    import httpx
    try:
        # Try to ping the API health endpoint
        with httpx.Client(timeout=timeout) as client:
            resp = client.get(f"{base_url}/docs")
            return resp.status_code == 200
    except (httpx.ConnectError, httpx.TimeoutException, Exception):
        return False

def _passes_genome_type_filter(genome_type: str, include_types: List[str], exclude_types: List[str]) -> bool:
    """Check if a genome type passes the filtering criteria"""
    if 'all' in include_types:
        # If 'all' is specified, only exclude explicitly excluded types
        return genome_type not in exclude_types

    # If specific types are included, check if genome_type is in the include list
    # and not in the exclude list
    return genome_type in include_types and genome_type not in exclude_types


def _detect_genome_type(title: str, accession: str) -> str:
    """Detect genome type from title and accession"""
    title_lower = title.lower() if title else ''
    accession_upper = accession.upper() if accession else ''

    # Check for plasmids first (most specific)
    if 'plasmid' in title_lower or 'plasmid' in accession_upper:
        return 'plasmid'

    # Check for complete genomes/chromosomes
    if ('complete genome' in title_lower or 'complete sequence' in title_lower or
        'chromosome' in title_lower or accession_upper.startswith('CP') or
        accession_upper.startswith('NC_') or accession_upper.startswith('NZ_CP')):
        return 'complete'

    # Check for scaffolds
    if 'scaffold' in title_lower:
        return 'scaffold'

    # Check for contigs
    if 'contig' in title_lower:
        return 'contig'

    # Default to chromosome if it looks like a main chromosome
    if 'chromosome' in title_lower and not any(x in title_lower for x in ['plasmid', 'scaffold', 'contig']):
        return 'chromosome'

    # If we can't determine, return unknown
    return 'unknown'


def main():
    parser = argparse.ArgumentParser(description="Download genomes from accession list with genome type filtering")
    parser.add_argument('accession_file', help='File containing accession numbers (one per line)')
    parser.add_argument('--max_genomes', type=int, default=None,
                        help='Maximum number of genomes to download (default: all)')
    parser.add_argument('--output_dir', default="./accession_list_genomes",
                        help='Output directory for downloaded files')
    parser.add_argument('--skip_metadata', action='store_true',
                        help='Skip metadata extraction (faster, but no BioSample/BioProject data)')
    parser.add_argument('--metadata_format', choices=['json', 'csv'], default='json',
                        help='Format for metadata output (default: json)')
    parser.add_argument('--genome_types', nargs='*',
                         choices=['complete', 'chromosome', 'scaffold', 'contig', 'plasmid', 'all'],
                         default=['all'],
                         help='Genome assembly types to include (default: all). Can specify multiple: --genome_types complete chromosome scaffold')
    parser.add_argument('--exclude_types', nargs='*',
                        choices=['complete', 'chromosome', 'scaffold', 'contig', 'plasmid'],
                        default=[],
                        help='Genome assembly types to exclude. Can specify multiple: --exclude_types plasmid contig')
    parser.add_argument('--parallel_downloads', type=int, default=6,
                        help='Number of parallel download workers (default: 6, max: 10)')
    parser.add_argument('--use-harvester', action='store_true',
                        help='Use NCBI-MetadataHarvester service for richer metadata extraction (requires service running at --harvester-url)')
    parser.add_argument('--harvester-url', default='http://127.0.0.1:8000',
                        help='Base URL for the metadata harvester service (default: http://127.0.0.1:8000)')

    args = parser.parse_args()

    # Start timing
    start_time = time.time()

    # Check if harvester service should be used
    use_harvester = args.use_harvester
    if use_harvester:
        print(f"Checking metadata harvester service at {args.harvester_url}...")
        if check_harvester_service(args.harvester_url):
            print("✓ Metadata harvester service is available")
        else:
            print(f"✗ WARNING: Metadata harvester service is NOT available at {args.harvester_url}")
            print("  Please start the service with:")
            print(f"    python -m uvicorn src.ncbi_metadata_harvester.main:app --host 127.0.0.1 --port 8000")
            print("  Falling back to standard metadata extraction...")
            use_harvester = False

    # Read accessions
    with open(args.accession_file, 'r') as f:
        all_accessions = [line.strip() for line in f if line.strip()]

    # Limit number of genomes if specified
    if args.max_genomes and args.max_genomes > 0:
        accessions = all_accessions[:args.max_genomes]
        print(f"Limited to first {args.max_genomes} accessions out of {len(all_accessions)} total")
    else:
        accessions = all_accessions

    print(f"Found {len(accessions)} accessions to download")
    print(f"Using {args.parallel_downloads} parallel workers for downloads")
    if args.metadata_format == 'csv':
        print("Metadata will be exported in CSV format")

    # Initialize extractor
    extractor = NCBIClient()

    # First, get metadata for all accessions to determine genome types
    if not args.skip_metadata:
        print("Extracting metadata to determine genome types...")
        try:
            if use_harvester:
                # Import harvester client
                from metadata_client import fetch_metadata_for_accessions
                
                print(f"Using metadata harvester service for {len(accessions)} accessions...")
                try:
                    metadata_records, errors = fetch_metadata_for_accessions(accessions, base_url=args.harvester_url)
                    if errors:
                        print(f"Note: {len(errors)} accessions had errors during harvester extraction")
                    print(f"Retrieved metadata for {len(metadata_records)} accessions from harvester")
                except Exception as harvester_error:
                    print(f"Warning: Harvester extraction failed ({harvester_error}), falling back to standard method")
                    use_harvester = False
                    metadata_records = extractor._extract_metadata_batch(accessions)
            else:
                metadata_records = extractor._extract_metadata_batch(accessions)
        except Exception as e:
            print(f"Warning: Metadata extraction failed ({e}), proceeding with genome type filtering disabled")
            args.skip_metadata = True
            metadata_records = []

        # Filter accessions based on genome types
        filtered_accessions = []
        for record in metadata_records:
            genome_type = _detect_genome_type(record.get('title', ''), record.get('accession', ''))
            if _passes_genome_type_filter(genome_type, args.genome_types, args.exclude_types):
                filtered_accessions.append(record['genome_id'])

        print(f"Filtered to {len(filtered_accessions)} accessions matching genome type criteria")
        if len(filtered_accessions) == 0:
            print("No accessions match the specified genome type filters. Exiting.")
            return

        accessions = filtered_accessions
    else:
        print("Warning: Skipping metadata extraction - genome type filtering will not be applied")

    # Download in parallel batches to avoid API limits
    from concurrent.futures import ThreadPoolExecutor, as_completed

    batch_size = 100
    for i in range(0, len(accessions), batch_size):
        batch = accessions[i:i + batch_size]
        print(f"Downloading batch {i//batch_size + 1}/{(len(accessions) + batch_size - 1)//batch_size} with {args.parallel_downloads} parallel workers")

        # Download FASTA files in parallel with rate limiting
        successful_downloads = 0
        failed_downloads = 0

        with ThreadPoolExecutor(max_workers=args.parallel_downloads) as executor:
            future_to_acc = {executor.submit(extractor.download_fasta, acc, args.output_dir): acc for acc in batch}

            for future in as_completed(future_to_acc):
                acc = future_to_acc[future]
                try:
                    success = future.result()
                    if success:
                        print(f"[OK] Downloaded {acc}")
                        successful_downloads += 1
                    else:
                        print(f"[FAIL] Failed {acc}")
                        failed_downloads += 1
                except Exception as e:
                    print(f"[ERROR] Error downloading {acc}: {e}")
                    failed_downloads += 1

                # Rate limiting: small delay between individual downloads
                time.sleep(0.1)

        print(f"Batch completed: {successful_downloads} successful, {failed_downloads} failed")

        # Small delay between batches to be respectful to NCBI
        time.sleep(1.0)

    # Extract metadata for all downloaded genomes (unless skipped)
    if not args.skip_metadata:
        downloaded_fasta = [f for f in os.listdir(args.output_dir) if f.endswith('.fasta')]
        downloaded_gbff = [f for f in os.listdir(args.output_dir) if f.endswith('.gbff')]
        genome_ids = [f.replace('.fasta', '') for f in downloaded_fasta]

        print(f"Extracting comprehensive metadata for {len(genome_ids)} downloaded genomes...")
        try:
            metadata_records = []
            
            if use_harvester:
                # Use harvester for all metadata - richer and more comprehensive
                from metadata_client import fetch_metadata_for_accessions
                
                print(f"Fetching enriched metadata from harvester service...")
                try:
                    metadata_records, errors = fetch_metadata_for_accessions(genome_ids, base_url=args.harvester_url)
                    if errors:
                        print(f"Note: {len(errors)} accessions had errors during harvester extraction")
                    print(f"Retrieved enriched metadata for {len(metadata_records)} genomes")
                except Exception as harvester_error:
                    print(f"Warning: Harvester metadata extraction failed ({harvester_error})")
                    print("Falling back to GBFF parsing + standard metadata extraction...")
                    use_harvester = False
                    # Fall through to standard method below
            
            if not use_harvester:
                # Use existing GBFF parsing + network backfill
                # Prefer parsing local GBFF files to avoid network errors
                gbff_map = {os.path.splitext(f)[0]: os.path.join(args.output_dir, f) for f in downloaded_gbff}
                for gid in genome_ids:
                    if gid in gbff_map:
                        meta = extractor.parse_gbff_metadata(gbff_map[gid])
                        # Ensure accession is consistent
                        meta['accession'] = gid
                        metadata_records.append(meta)
                    else:
                        # Fallback to network metadata if GBFF not present
                        pass

                # If some genomes lack GBFF, optionally backfill via eutils
                missing = [gid for gid in genome_ids if gid not in gbff_map]
                if missing:
                    try:
                        network_records = extractor._extract_metadata_batch(missing)
                        metadata_records.extend(network_records)
                    except Exception as e:
                        print(f"Note: Network metadata backfill failed for {len(missing)} genomes: {e}")

            # Add genome type information to final metadata
            for record in metadata_records:
                genome_type = _detect_genome_type(record.get('title', ''), record.get('accession', ''))
                record['genome_type'] = genome_type

            # Debug: Print comprehensive metadata for first record
            if metadata_records:
                first_record = metadata_records[0]
                print(f"\n=== COMPREHENSIVE METADATA SAMPLE ===")
                print(f"Accession: {first_record.get('accession')}")
                print(f"Title: {first_record.get('title')}")
                print(f"Organism: {first_record.get('organism')}")
                print(f"BioSample: {first_record.get('biosample')}")
                print(f"BioProject: {first_record.get('bioproject')}")
                print(f"Collection Date: {first_record.get('collection_date')}")
                print(f"Country: {first_record.get('country')}")
                print(f"Host: {first_record.get('host')}")
                print(f"Isolation Source: {first_record.get('isolation_source')}")
                print(f"MIC Data: {len(first_record.get('mic_data', []))} entries")
                print(f"Antibiotic Resistance: {len(first_record.get('antibiotic_resistance', []))} entries")
                print(f"Genome Type: {first_record.get('genome_type')}")
                print(f"Quality Score: {first_record.get('quality_score')}")
                print(f"All metadata keys: {sorted(first_record.keys())}")
                print("=" * 50)

            # Save metadata in requested format
            if args.metadata_format == 'json':
                import json
                with open(os.path.join(args.output_dir, "accession_metadata.json"), 'w') as f:
                    json.dump(metadata_records, f, indent=2)
            else:
                # Save as CSV
                save_metadata_to_csv(metadata_records, os.path.join(args.output_dir, "accession_metadata.csv"))

            # Calculate and display elapsed time
            elapsed_time = time.time() - start_time
            minutes = int(elapsed_time // 60)
            seconds = int(elapsed_time % 60)
            print(f"\n{'='*60}")
            print(f"Downloaded {len(downloaded_fasta)} genomes with complete metadata to {args.output_dir}")
            print(f"Total time: {minutes}m {seconds}s ({elapsed_time:.2f} seconds)")
            print(f"{'='*60}")
        except Exception as e:
            # Calculate and display elapsed time even on partial failure
            elapsed_time = time.time() - start_time
            minutes = int(elapsed_time // 60)
            seconds = int(elapsed_time % 60)
            print(f"\n{'='*60}")
            print(f"Warning: Final metadata extraction failed ({e}), but FASTA files were downloaded successfully")
            print(f"Downloaded {len(downloaded_fasta)} genomes to {args.output_dir}")
            print(f"Total time: {minutes}m {seconds}s ({elapsed_time:.2f} seconds)")
            print(f"{'='*60}")
            # Create empty metadata CSV as fallback
            if args.metadata_format == 'csv':
                empty_records = []
                for gid in genome_ids:
                    empty_records.append({
                        'genome_id': gid,
                        'accession': gid,
                        'organism': None,
                        'genus': None,
                        'species': None,
                        'title': None,
                        'biosample': None,
                        'bioproject': None,
                        'collection_date': None,
                        'country': None,
                        'host': None,
                        'isolation_source': None,
                        'mic_data': [],
                        'antibiotic_resistance': [],
                        'genome_type': 'unknown',
                        'quality_score': 0,
                        'resistance_phenotype': []
                    })
                save_metadata_to_csv(empty_records, os.path.join(args.output_dir, "accession_metadata.csv"))
                print("Created empty metadata CSV file")
    else:
        downloaded_fasta = [f for f in os.listdir(args.output_dir) if f.endswith('.fasta')]
        # Calculate and display elapsed time when metadata is skipped
        elapsed_time = time.time() - start_time
        minutes = int(elapsed_time // 60)
        seconds = int(elapsed_time % 60)
        print(f"\n{'='*60}")
        print(f"Downloaded {len(downloaded_fasta)} genomes to {args.output_dir} (metadata skipped)")
        print(f"Total time: {minutes}m {seconds}s ({elapsed_time:.2f} seconds)")
        print(f"{'='*60}")

def save_metadata_to_csv(records: List[Dict[str, Any]], output_file: str):
    """Save metadata to CSV format with enhanced field handling"""
    if not records:
        return

    # Flatten nested structures for CSV
    flattened_data = []
    for item in records:
        flat_item = {}
        for key, value in item.items():
            if isinstance(value, list):
                if key == 'amr_phenotypes':
                    flat_item[key] = '; '.join(value) if value else ''
                elif key == 'mic_data':
                    mic_strings = []
                    for mic in value:
                        if isinstance(mic, dict):
                            antibiotic = mic.get('antibiotic', 'Unknown')
                            mic_value = mic.get('mic_value', 'Unknown')
                            mic_strings.append(f"{antibiotic}: {mic_value}")
                        else:
                            mic_strings.append(str(mic))
                    flat_item[key] = '; '.join(mic_strings) if mic_strings else ''
                elif key == 'antibiotic_resistance':
                    resistance_strings = []
                    for res in value:
                        if isinstance(res, dict):
                            antibiotic = res.get('antibiotic', 'Unknown')
                            resistance = res.get('resistance', 'Unknown')
                            resistance_strings.append(f"{antibiotic}: {resistance}")
                        else:
                            resistance_strings.append(str(res))
                    flat_item[key] = '; '.join(resistance_strings) if resistance_strings else ''
                else:
                    flat_item[key] = '; '.join(str(v) for v in value) if value else ''
            elif isinstance(value, dict):
                for subkey, subvalue in value.items():
                    flat_item[f"{key}_{subkey}"] = str(subvalue) if subvalue is not None else ''
            else:
                flat_item[key] = str(value) if value is not None else ''
        flattened_data.append(flat_item)

    # Define preferred field order
    priority_fields = [
        'accession', 'genome_id', 'organism', 'genus', 'species', 'strain', 'title',
        'biosample', 'bioproject', 'collection_date', 'country', 'host', 'isolation_source',
        'amr_phenotypes', 'mic_data', 'antibiotic_resistance', 'genome_type'
    ]

    # Get all available fields
    all_fields = set()
    for item in flattened_data:
        all_fields.update(item.keys())

    # Order fields: priority first, then alphabetical
    fieldnames = []
    for field in priority_fields:
        if field in all_fields:
            fieldnames.append(field)
            all_fields.remove(field)

    fieldnames.extend(sorted(all_fields))

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(flattened_data)


if __name__ == "__main__":
    main()