import os
import subprocess
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm

# Set directories
input_bam_dir = ""
sorted_bam_dir = ""
marked_bam_dir = ""
picard_path = ""

os.makedirs(sorted_bam_dir, exist_ok=True)
os.makedirs(marked_bam_dir, exist_ok=True)


def sort_bam(input_bam):
    """Sort BAM file by coordinate using samtools."""
    sample_name = os.path.basename(input_bam).replace(".bam", "")
    sorted_bam = os.path.join(sorted_bam_dir, f"{sample_name}.sorted.bam")

    if os.path.exists(sorted_bam):
        print(f"Sorted BAM already exists for {sample_name}. Skipping sorting.")
        return sorted_bam

    cmd = f"samtools sort -o {sorted_bam} {input_bam}"
    try:
        subprocess.run(cmd, check=True, shell=True)
        print(f"Sorted BAM created: {sorted_bam}")
        return sorted_bam
    except subprocess.CalledProcessError as e:
        print(f"Sorting failed for {input_bam}: {e}")
        return None


def mark_duplicates(input_bam):
    """Mark duplicates using Picard."""
    if input_bam is None:
        return

    sample_name = os.path.basename(input_bam).replace(".bam", "").replace(".sorted", "")
    output_bam = os.path.join(marked_bam_dir, f"{sample_name}.marked.bam")
    metrics_file = os.path.join(marked_bam_dir, f"{sample_name}_metrics.txt")

    if os.path.exists(output_bam):
        print(f"Marked BAM already exists for {sample_name}. Skipping marking.")
        return

    cmd = (
        f"java -jar {picard_path} MarkDuplicates "
        f"I={input_bam} O={output_bam} M={metrics_file} "
        f"REMOVE_DUPLICATES=false VALIDATION_STRINGENCY=LENIENT"
    )
    try:
        subprocess.run(cmd, check=True, shell=True)
        print(f"Marked duplicates for {sample_name}. Output: {output_bam}")
    except subprocess.CalledProcessError as e:
        print(f"MarkDuplicates failed for {input_bam}: {e}")


def main():
    bam_files = [os.path.join(input_bam_dir, f) for f in os.listdir(input_bam_dir) if f.endswith(".bam")]

    print(f"Found {len(bam_files)} BAM files.")

    # Step 1: Sort all BAM files
    print("\n Sorting BAM files...")
    with ProcessPoolExecutor(max_workers=5) as executor:
        sorted_bam_files = list(tqdm(executor.map(sort_bam, bam_files), total=len(bam_files), desc="Sorting BAMs"))

    # Filter out any failed ones
    sorted_bam_files = [f for f in sorted_bam_files if f is not None]

    # Step 2: Mark duplicates
    print("\n Marking duplicates...")
    with ProcessPoolExecutor(max_workers=5) as executor:
        list(tqdm(executor.map(mark_duplicates, sorted_bam_files), total=len(sorted_bam_files), desc="Marking Duplicates"))


if __name__ == "__main__":
    main()
