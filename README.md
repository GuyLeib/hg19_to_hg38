# hg19_to_hg38

Step 1 : 
BAM to FASTQ with GATK
script: bam_to_fastq_gatk.py

Step 2:
STAR alignment - hg38 GDC refrences. 
script: star_aligner_gdc.py

Step 3:
MarkDuplicates - sorting and running markduplicates on the bams 
script: 

Step 4: 
RNASEQC
script: srnaseqc_gdc.py
