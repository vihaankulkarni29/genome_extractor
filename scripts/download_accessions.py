#!/usr/bin/env python3
"""
Download genomes from accession list with genome type filtering
"""

import os
import sys
import argparse
from typing import List
from clients.ncbi_client import NCBIClient

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
    parser.add_argument('--genome_types', nargs='*',
                         choices=['complete', 'chromosome', 'scaffold', 'contig', 'plasmid', 'all'],
                         default=['all'],
                         help='Genome assembly types to include (default: all). Can specify multiple: --genome_types complete chromosome scaffold')
    parser.add_argument('--exclude_types', nargs='*',
                         choices=['complete', 'chromosome', 'scaffold', 'contig', 'plasmid'],
                         default=[],
                         help='Genome assembly types to exclude. Can specify multiple: --exclude_types plasmid contig')

    args = parser.parse_args()

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

    # Initialize extractor
    extractor = NCBIClient()

    # First, get metadata for all accessions to determine genome types
    if not args.skip_metadata:
        print("Extracting metadata to determine genome types...")
        metadata_records = extractor._extract_metadata_batch(accessions)

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

    # Download in batches to avoid API limits
    batch_size = 100
    for i in range(0, len(accessions), batch_size):
        batch = accessions[i:i + batch_size]
        print(f"Downloading batch {i//batch_size + 1}/{(len(accessions) + batch_size - 1)//batch_size}")

        # Download FASTA files
        for acc in batch:
            try:
                success = extractor.download_fasta(acc, args.output_dir)
                if success:
                    print(f"[OK] Downloaded {acc}")
                else:
                    print(f"[FAIL] Failed {acc}")
            except Exception as e:
                print(f"[ERROR] Error downloading {acc}: {e}")

    # Extract metadata for all downloaded genomes (unless skipped)
    if not args.skip_metadata:
        downloaded_files = [f for f in os.listdir(args.output_dir) if f.endswith('.fasta')]
        genome_ids = [f.replace('.fasta', '') for f in downloaded_files]

        print(f"Extracting final metadata for {len(genome_ids)} downloaded genomes...")
        # Use the correct internal method for specific genome IDs
        metadata_records = extractor._extract_metadata_batch(genome_ids)

        # Add genome type information to final metadata
        for record in metadata_records:
            genome_type = _detect_genome_type(record.get('title', ''), record.get('accession', ''))
            record['genome_type'] = genome_type

        # Save metadata to JSON
        import json
        with open(os.path.join(args.output_dir, "accession_metadata.json"), 'w') as f:
            json.dump(metadata_records, f, indent=2)

        print(f"Downloaded {len(downloaded_files)} genomes with complete metadata to {args.output_dir}")
    else:
        downloaded_files = [f for f in os.listdir(args.output_dir) if f.endswith('.fasta')]
        print(f"Downloaded {len(downloaded_files)} genomes to {args.output_dir} (metadata skipped)")

if __name__ == "__main__":
    main()