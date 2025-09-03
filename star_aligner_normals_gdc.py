import subprocess
import os
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm

def align_reads(fq1, fq2, output_prefix, star_index, gtf_file="/data01/private/resources/to_sort/gencode.v36.annotation.gtf"):
    """Align reads to GRCh38 using STAR."""
    print(f"Aligning reads for {output_prefix}...")
    cmd = (
        f"STAR --genomeDir {star_index} "
        f"--readFilesIn {fq1} {fq2} "
        f"--outSAMattrRGline ID:rg1 SM:sm1 " 
        f"--readFilesCommand zcat "
        f"--runThreadN 8 "
        f"--twopassMode Basic "
        f"--outFilterMultimapNmax 20 "
        f"--alignSJoverhangMin 8 "
        f"--alignSJDBoverhangMin 1 "
        f"--outFilterMismatchNmax 999 "
        f"--outFilterMismatchNoverLmax 0.1 "
        f"--alignIntronMin 20 "
        f"--alignIntronMax 1000000 "
        f"--alignMatesGapMax 1000000 "
        f"--outFilterType BySJout "
        f"--outFilterScoreMinOverLread 0.33 "
        f"--outFilterMatchNminOverLread 0.33 "
        f"--limitSjdbInsertNsj 1200000 "
        f"--outFileNamePrefix {output_prefix} "
        f"--outSAMstrandField intronMotif "
        f"--outFilterIntronMotifs None "
        f"--alignSoftClipAtReferenceEnds Yes "
        f"--quantMode TranscriptomeSAM GeneCounts "
        f"--outSAMtype BAM Unsorted "
        f"--outSAMunmapped Within "
        f"--genomeLoad NoSharedMemory "
        f"--chimSegmentMin 15 "
        f"--chimJunctionOverhangMin 15 "
        f"--chimOutType Junctions WithinBAM SoftClip "
        f"--chimOutJunctionFormat 1 "
        f"--chimMainSegmentMultNmax 1 "
        f"--outSAMattributes NH HI AS nM NM ch "
    )
    try:
        subprocess.run(cmd, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error aligning {output_prefix}: {e}. Continuing with next file.")



def process_sample(args):
    fastq_dir, sample_name, star_index, output_dir = args

    fq1 = os.path.join(fastq_dir, f"{sample_name}_1.fastq")
    fq2 = os.path.join(fastq_dir, f"{sample_name}_2.fastq")
    output_prefix = os.path.join(output_dir, sample_name)
    final_output_file = output_prefix + ".Aligned.sortedByCoord.out.bam"

    if os.path.exists(final_output_file):
        print(f"Output for {sample_name} already exists. Skipping.")
        return

    align_reads(fq1, fq2, output_prefix, star_index)


def get_sample_names_from_fastq_dir(fastq_dir):
    files = [f for f in os.listdir(fastq_dir) if f.endswith('.fastq')]
    sample_names = [f.split("_")[0] for f in files]
    return sorted(sample_names)


def multiprocess_handler(samples, fastq_dir, star_index, output_dir):
    with ProcessPoolExecutor(max_workers=len(samples)) as executor:
        args = [(fastq_dir, sample, star_index, output_dir) for sample in samples]
        for _ in tqdm(executor.map(process_sample, args), total=len(samples), desc="Aligning samples"):
            pass


def main(fastq_dir, star_index, output_dir):
    sample_names = get_sample_names_from_fastq_dir(fastq_dir)
    groups = [sample_names[i:i + 5] for i in range(0, len(sample_names), 5)]
    for group in groups:
        multiprocess_handler(group, fastq_dir, star_index, output_dir)


if __name__ == "__main__":
    star_index = "/data01/private/resources/to_sort/hg38_gdc/STAR_index/star-2.7.5c_GRCh38.d1.vd1_gencode.v36/"
    output_dir = ""
    fastq_dir = ""

    main(fastq_dir, star_index, output_dir)