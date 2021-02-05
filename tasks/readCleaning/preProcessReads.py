import luigi
import time
import os
import subprocess
from tasks.readCleaning.cleanedReadQC import *
from tasks.readCleaning.rawReadQC import *

class GlobalParameter(luigi.Config):

	project_name=luigi.Parameter()
	rnaseq_dir=luigi.Parameter()
	read_suffix=luigi.Parameter()
	adapter=luigi.Parameter()
	threads=luigi.Parameter()
	maxMemory=luigi.Parameter()
	read_library_type=luigi.Parameter()
	threads = luigi.Parameter()
	maxMemory = luigi.Parameter()
	adapter=luigi.Parameter()
	

def run_cmd(cmd):
	p = subprocess.Popen(cmd, bufsize=-1,
				 shell=True,
				 universal_newlines=True,
				 stdout=subprocess.PIPE,
				 executable='/bin/bash')
	output = p.communicate()[0]
	return output

def createFolder(directory):
	try:
		if not os.path.exists(directory):
			os.makedirs(directory)
	except OSError:
		print ('Error: Creating directory. ' + directory)

createFolder("task_logs")

class bbduk(luigi.Task):
	projectName=GlobalParameter().project_name
	read_library_type=GlobalParameter().read_library_type
	rnaseq_dir=GlobalParameter().rnaseq_dir
	read_suffix=GlobalParameter().read_suffix

	threads = GlobalParameter().threads
	maxMemory = GlobalParameter().maxMemory
	sampleName = luigi.Parameter(description="name of the sample to be analyzed. (string)")
	adapter = GlobalParameter().adapter

	kmer_length=luigi.OptionalParameter(default="21",description="Kmer length used for finding contaminants. Contaminants "
												 "shorter than kmer length will not be found.Default: 21")

	corret_error=luigi.BoolParameter(default=False,description="Perform Error Correction Or Not")

	k_trim = luigi.ChoiceParameter(default="r",description="Trimming protocol to remove bases matching reference kmers from reads. "
											   "Choose From['f: dont trim','r: trim to right','l: trim to left]",
								   choices=["f", "r", "l"], var_type=str)

	quality_trim=luigi.ChoiceParameter(default="lr",description="Trimming protocol to remove bases with quality below the minimum "
												   "average region quality from read ends. Performed after looking for kmers."
												   " If enabled, set also 'Average quality below which to trim region'. "
												   "Choose From['f: trim neither end', 'rl: trim both end','r: trim only right end','l: trim only left end]",
								   choices=["f", "lr", "r","l","w"], var_type=str)

	trim_quality = luigi.IntParameter(description="Average quality below which to trim region ",default=6)


	min_length = luigi.OptionalParameter(default="20",description="reads shorter than min_length will be discarded. Default: "
													"min_length=20")

	min_average_quality = luigi.OptionalParameter(default="10", description="Reads with average quality (after trimming) below "
																"this will be discarded. Default: min_average_quality=10")
	min_base_quality = luigi.OptionalParameter(default="0",description="Reads with any base below this quality (after trimming) will be discarded. "
																"Default: min_base_quality=0")
	mingc = luigi.OptionalParameter(default="0.0", description="Discard reads with GC content below this. Default: min_gc=0.001")
	maxgc = luigi.OptionalParameter(default="1.0", description="Discard reads with GC content below this. Default: max_gc=0.999")
	kmer = luigi.OptionalParameter(default="13", description="Kmer length used for finding contaminants. Default: kmer=13")

	trim_front = luigi.Parameter(default="0", description="trimming how many bases in front for read. Default: "
														  "trim_front=0")
	trim_tail = luigi.Parameter(default="0", description="trimming how many bases in tail for read. Default: "
														  "trim_tail=0")
	max_n=luigi.IntParameter(default=-1,description="Maximum number of Ns after trimming [maxns=-1]. "
												   "If non-negative, reads with more Ns than this (after trimming) will be discarded.")

	trim_by_overlap=luigi.Parameter(default="f",description="Trim adapters based on where paired-end reads overlap [tbo]")
	trim_pairs_evenly=luigi.Parameter(default="f", description="Trim both sequences of paired-end reads to the minimum length of either sequence ["
															   "tpe]")

	
	def output(self):
		se_clean_read_folder = os.path.join(os.getcwd(), self.projectName,"ReadQC", "Cleaned_SE_Reads" + "/")
		pe_clean_read_folder = os.path.join(os.getcwd(), self.projectName,"ReadQC", "Cleaned_PE_Reads" + "/")
		
		if self.read_library_type == "pe":
			return {'out1': luigi.LocalTarget(pe_clean_read_folder + self.sampleName + "_R1.fastq"),
					'out2': luigi.LocalTarget(pe_clean_read_folder + self.sampleName + "_R2.fastq")}

		if self.read_library_type == "se":
			return {'out1': luigi.LocalTarget(se_clean_read_folder + self.sampleName + ".fastq")}

		
		
	def run(self):
		se_clean_read_folder = os.path.join(os.getcwd(), self.projectName,"ReadQC","Cleaned_SE_Reads" + "/")
		pe_clean_read_folder = os.path.join(os.getcwd(), self.projectName,"ReadQC","Cleaned_PE_Reads" + "/")
		

		read_clean_log_folder = os.path.join(os.getcwd(), "log","ReadCleaning" + "/")

		bbduk_se_clean_stat_folder = os.path.join(os.getcwd(), self.projectName,"ReadQC", "Cleaned_SE_Reads_STAT" + "/")
		bbduk_pe_clean_stat_folder = os.path.join(os.getcwd(), self.projectName, "ReadQC","Cleaned_PE_Reads_STAT" + "/")

		
		cmd_clean_pe = "[ -d  {pe_clean_read_folder} ] || mkdir -p {pe_clean_read_folder}; " \
					   "mkdir -p {bbduk_pe_clean_stat_folder}; mkdir -p {read_clean_log_folder}; bbduk.sh " \
					   "-Xmx{Xmx}g " \
					   "threads={cpu} " \
					   "ecco={corret_error} " \
					   "minlength={min_length} " \
					   "minavgquality={min_average_quality} " \
					   "minbasequality={min_base_quality} " \
					   "trimq={trim_quality} " \
					   "qtrim={quality_trim} " \
					   "ftl={trim_front} " \
					   "ftr2={trim_tail} " \
					   "mingc={mingc} " \
					   "maxgc={maxgc} " \
					   "maxns={max_n} " \
					   "tbo={trim_by_overlap} " \
					   "tpe={trim_pairs_evenly} " \
					   "in1={rnaseq_dir}{sampleName}_R1.{read_suffix} " \
					   "in2={rnaseq_dir}{sampleName}_R2.{read_suffix} " \
					   "out={pe_clean_read_folder}{sampleName}_R1.fastq " \
					   "out2={pe_clean_read_folder}{sampleName}_R2.fastq " \
					   "outs={pe_clean_read_folder}{sampleName}.fastq " \
					   "ziplevel=9 " \
					   "ref={adapter} " \
					   "stats={bbduk_pe_clean_stat_folder}{sampleName}.stat " \
					   "bqhist={bbduk_pe_clean_stat_folder}{sampleName}.qual.hist " \
					   "gchist={bbduk_pe_clean_stat_folder}{sampleName}.gc.hist " \
					   " 2>{read_clean_log_folder}{sampleName}_pe_bbduk_run.log "\
			.format(Xmx=GlobalParameter().maxMemory,
					cpu=GlobalParameter().threads,
					rnaseq_dir=os.path.join(GlobalParameter().rnaseq_dir),
					read_suffix=GlobalParameter().read_suffix,
					sampleName=self.sampleName,
					corret_error=self.corret_error,
					adapter=self.adapter,
					pe_clean_read_folder=pe_clean_read_folder,
					min_length=self.min_length,
					min_average_quality=self.min_average_quality,
					min_base_quality=self.min_base_quality,
					trim_quality=self.trim_quality,
					quality_trim=self.quality_trim,
					trim_front=self.trim_front,
					trim_tail=self.trim_tail,
					mingc=self.mingc, max_n=self.max_n,
					maxgc=self.maxgc,
					kmer=self.kmer,
					trim_by_overlap=self.trim_by_overlap,
					trim_pairs_evenly=self.trim_pairs_evenly,
					bbduk_pe_clean_stat_folder=bbduk_pe_clean_stat_folder,
					read_clean_log_folder=read_clean_log_folder)

		##################
		
		cmd_clean_se = "[ -d  {se_clean_read_folder} ] || mkdir -p {se_clean_read_folder}; " \
					   "mkdir -p {bbduk_se_clean_stat_folder}; mkdir -p {read_clean_log_folder}; bbduk.sh " \
					   "-Xmx{Xmx}g " \
					   "threads={cpu} " \
					   "minlength={min_length} " \
					   "minavgquality={min_average_quality} " \
					   "minbasequality={min_base_quality} " \
					   "mingc={mingc} " \
					   "maxgc={maxgc} " \
					   "trimq={trim_quality} " \
					   "qtrim={quality_trim} " \
					   "ftl={trim_front} " \
					   "ftr2={trim_tail} " \
					   "in={rnaseq_dir}{sampleName}.{read_suffix} " \
					   "out={se_clean_read_folder}{sampleName}.fastq " \
					   "ziplevel=9 ref={adapter} " \
					   "stats={bbduk_se_clean_stat_folder}{sampleName}.stat " \
					   "bqhist={bbduk_se_clean_stat_folder}{sampleName}.qual.hist " \
					   "gchist={bbduk_se_clean_stat_folder}{sampleName}.gc.hist " \
					   " 2>{read_clean_log_folder}{sampleName}_se_bbduk_run.log ".format(Xmx=GlobalParameter().maxMemory,
											rnaseq_dir=GlobalParameter().rnaseq_dir,
					   						read_suffix=GlobalParameter().read_suffix,
											cpu=GlobalParameter().threads,
											sampleName=self.sampleName,
											adapter=self.adapter,
											se_clean_read_folder=se_clean_read_folder,
											trim_quality=self.trim_quality,
											quality_trim=self.quality_trim,
											min_average_quality=self.min_average_quality,
											min_base_quality=self.min_base_quality,
											trim_front=self.trim_front,
											trim_tail=self.trim_tail,
											min_length=self.min_length,
											mingc=self.mingc,
											maxgc=self.maxgc,
											kmer=self.kmer,
											read_clean_log_folder=read_clean_log_folder,
											bbduk_se_clean_stat_folder=bbduk_se_clean_stat_folder)

		if self.read_library_type == "pe":
			print("****** NOW RUNNING COMMAND ******: " + cmd_clean_pe)
			print(run_cmd(cmd_clean_pe))

		if self.read_library_type == "se":
			print("****** NOW RUNNING COMMAND ******: " + cmd_clean_se)
			print(run_cmd(cmd_clean_se))
		

		
class cleanReads(luigi.Task):
	
	read_library_type = GlobalParameter().read_library_type
	def requires(self):

		if self.read_library_type == "pe":
			return [

					[bbduk(
						sampleName=i)
					for i in [line.strip()
							  for line in
							  open((os.path.join(os.getcwd(), "config", "pe_samples.lst")))]],

					[readqc(
							sampleName=i)
					for i in [line.strip()
							  for line in
							  open((os.path.join(os.getcwd(), "config", "pe_samples.lst")))]]
			       ]


		if self.read_library_type == "se":
			return [[bbduk(
						sampleName=i)
					for i in [line.strip()
							  for line in
							  open((os.path.join(os.getcwd(), "config", "se_samples.lst")))]],

					[readqc(
							sampleName=i)
					 for i in [line.strip()
							   for line in
							   open((os.path.join(os.getcwd(), "config", "se_samples.lst")))]]
					]


		
	def output(self):
		timestamp = time.strftime('%Y%m%d.%H%M%S', time.localtime())
		return luigi.LocalTarget(os.path.join(os.getcwd(),"task_logs",'task.clean.shortread.complete.{t}'.format(t=timestamp)))

	def run(self):
		timestamp = time.strftime('%Y%m%d.%H%M%S', time.localtime())
		with self.output().open('w') as outfile:
			outfile.write('short read processing finished at {t}'.format(t=timestamp))
