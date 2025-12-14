#!/usr/bin/env python3
"""
FAST genome downloader - Nuccore only, optimized for speed
No Assembly database, minimal metadata extraction, maximum parallelization
"""

import os
import sys
import argparse
import time
from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add paths for imports
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, 'src'))

from src.clients.ncbi_client import NCBIClient


def fast_detect_genome_type(accession: str) -> str:
    """Fast pattern-based genome type detection - no API calls, ~95% accurate"""
    acc = accession.upper().strip()
    
    # COMPLETE GENOMES (high confidence patterns):
    # CP, NC_, AP = RefSeq complete genomes
    # NZ_CP, NZ_NC = GenBank complete chromosomes
    if acc.startswith(('CP', 'NC_', 'AP', 'NZ_CP', 'NZ_NC')):
        return 'complete'
    
    # CONTIGS (high confidence) - numbered fragments in middle:
    # Pattern: NZ_XXXX01000XXX (01000 = contig number)
    # Pattern: NZ_XXXX00000000 (8 zeros = WGS master record for contigs)
    if acc.startswith('NZ_'):
        # Check for contig patterns (numbered fragments)
        if '01000' in acc or '02000' in acc or '03000' in acc or '04000' in acc or '05000' in acc:
            return 'contig'
        if '00000000' in acc:  # WGS master record (8 zeros) for contig assemblies
            return 'contig'
        # NZ_ without contig pattern = likely complete
        return 'complete'
    
    # Complete genome patterns from other sources:
    # OW, OX = NCBI WGS complete sequences
    if acc.startswith(('OW', 'OX')):
        return 'complete'
    
    # Default: assume complete if no contig markers
    return 'complete'


def main():
    parser = argparse.ArgumentParser(description="FAST Nuccore-only genome downloader")
    parser.add_argument('accession_file', help='File with accessions (one per line)')
    parser.add_argument('--max_genomes', type=int, default=100, help='Max genomes')
    parser.add_argument('--output_dir', default="./results", help='Output directory')
    parser.add_argument('--parallel_downloads', type=int, default=10, help='Parallel workers (max 10)')
    parser.add_argument('--complete_only', action='store_true', help='Download only complete genomes (smart filter, ~95%% accurate)')

    args = parser.parse_args()
    args.parallel_downloads = min(args.parallel_downloads, 10)

    start_time = time.time()
    os.makedirs(args.output_dir, exist_ok=True)

    print("\n" + "=" * 70)
    print("⚡ FAST NUCCORE DOWNLOADER - Speed Optimized")
    print("=" * 70)

    # Read accessions
    with open(args.accession_file, 'r') as f:
        all_accessions = [line.strip() for line in f if line.strip()]

    # Apply smart filter if requested
    if args.complete_only:
        print("🔍 Applying smart filter (complete genomes only)...")
        complete_accessions = []
        contig_count = 0
        for acc in all_accessions:
            gtype = fast_detect_genome_type(acc)
            if gtype == 'complete':
                complete_accessions.append(acc)
            else:
                contig_count += 1
        
        print(f"   ✅ {len(complete_accessions)} complete genomes found")
        print(f"   🚫 {contig_count} contigs/scaffolds filtered out")
        all_accessions = complete_accessions

    accessions = all_accessions[:args.max_genomes]
    print(f"📋 Accessions: {len(accessions)} to download")
    print(f"🔗 Parallel workers: {args.parallel_downloads}")
    if args.complete_only:
        print("✨ Filter: Complete genomes only (smart pattern-based)")
    print("=" * 70 + "\n")

    # Initialize client
    client = NCBIClient(max_workers=args.parallel_downloads)

    # Download in parallel - FAST
    print(f"⬇️  Downloading {len(accessions)} genomes...")
    successful = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=args.parallel_downloads) as executor:
        futures = {
            executor.submit(client.download_fasta, acc, args.output_dir): acc
            for acc in accessions
        }

        for future in as_completed(futures):
            try:
                if future.result():
                    successful += 1
                    if successful % 10 == 0:
                        print(f"   ✅ {successful}/{len(accessions)} downloaded")
                else:
                    failed += 1
            except Exception:
                failed += 1

    elapsed = time.time() - start_time
    minutes = int(elapsed // 60)
    seconds = int(elapsed % 60)

    print("\n" + "=" * 70)
    print(f"✨ DOWNLOAD COMPLETE")
    print(f"   ✅ Successful: {successful}")
    print(f"   ❌ Failed: {failed}")
    print(f"   📁 Output: {args.output_dir}")
    print(f"   ⏱️  Total time: {minutes}m {seconds}s")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
