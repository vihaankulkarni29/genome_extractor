#!/usr/bin/env python3
import os
import sys
import json
import csv
import argparse
from pathlib import Path

script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
sys.path.insert(0, project_root)

def get_fasta_metadata(fasta_file: Path) -> dict:
    accession = fasta_file.name.replace('.fasta', '').replace('.fa', '')
    try:
        with open(fasta_file, 'r', encoding='utf-8', errors='ignore') as f:
            header = f.readline().strip()
            file_size = fasta_file.stat().st_size
            seq_count = sum(1 for line in f if line.startswith('>'))
            
            return {
                'accession': accession,
                'file_name': fasta_file.name,
                'file_size_MB': round(file_size / (1024*1024), 2),
                'sequences': seq_count + 1 if header.startswith('>') else seq_count,
                'header': header[:80],
                'status': 'OK'
            }
    except Exception as e:
        return {
            'accession': accession,
            'file_name': fasta_file.name,
            'file_size_MB': 'N/A',
            'sequences': 'N/A',
            'header': 'N/A',
            'status': 'Failed'
        }

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('input_dir')
    parser.add_argument('--output', '-o', default='metadata.csv')
    parser.add_argument('--output_json', '-j')
    args = parser.parse_args()
    
    input_dir = Path(args.input_dir)
    fasta_files = sorted(list(input_dir.glob('*.fasta')) + list(input_dir.glob('*.fa')))
    
    print("[INFO] Found {} FASTA files".format(len(fasta_files)))
    
    metadata_list = [get_fasta_metadata(f) for f in fasta_files]
    
    with open(args.output, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=metadata_list[0].keys())
        writer.writeheader()
        writer.writerows(metadata_list)
    
    if args.output_json:
        with open(args.output_json, 'w') as f:
            json.dump(metadata_list, f, indent=2)
    
    print("[OK] Metadata saved to {} and {}".format(args.output, args.output_json or 'N/A'))

if __name__ == '__main__':
    main()
