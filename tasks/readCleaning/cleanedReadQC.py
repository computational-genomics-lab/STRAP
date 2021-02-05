import luigi
import time
import os
import subprocess

class GlobalParameter(luigi.Config):




	
	paired_end_read_suffix = luigi.Parameter()
	single_end_read_suffix = luigi.Parameter()
	paired_end_interleaved_read_suffix = luigi.Parameter()
	mate_pair_read_suffix = luigi.Parameter()
	mate_pair_interleaved_read_suffix = luigi.Parameter()
	long_read_suffix = luigi.Parameter()
	threads = luigi.Parameter()
	maxMemory = luigi.Parameter()
	

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

class postreadqc(luigi.Task):
	#projectName = luigi.Parameter(default="ReadQC")
	sampleName = luigi.Parameter(description="name of the sample to be analyzed. (string)")
	read_library_type = luigi.ChoiceParameter(choices=["lr","se","pe", "mp","pe-mp","pe-mp-se","pe-lr","pe-mp-lr",],var_type=str)




	def output(self):
		se_readQC_folder = os.path.join(os.getcwd(), "ReadQC", "PostQC-SE-Reads" + "/")
		pe_readQC_folder = os.path.join(os.getcwd(), "ReadQC", "PostQC-PE-Reads" + "/")
		mp_readQC_folder = os.path.join(os.getcwd(), "ReadQC", "PostQC-MP-Reads" + "/")
		lr_readQC_folder = os.path.join(os.getcwd(), "ReadQC", "PostQC-LONG-Reads" + "/")
		
		if self.read_library_type == "pe":
			return {'out1': luigi.LocalTarget(pe_readQC_folder + self.sampleName + "_R1_fastqc.html"),
					'out2': luigi.LocalTarget(pe_readQC_folder + self.sampleName + "_R2_fastqc.html")}

		if self.read_library_type == "se":
			return {'out1': luigi.LocalTarget(se_readQC_folder + self.sampleName + "_fastqc.html")}

		if self.read_library_type == "lr":
			return {'out1': luigi.LocalTarget(lr_readQC_folder + self.sampleName + "_nanoQC.html")}

		if self.read_library_type == "mp":
			return {'out1': luigi.LocalTarget(mp_readQC_folder + self.sampleName + "_R1_fastqc.html"),
					'out2': luigi.LocalTarget(mp_readQC_folder + self.sampleName + "_R1_fastqc.html")}

		if self.read_library_type == "se":
			return {'out1': luigi.LocalTarget(se_readQC_folder + self.sampleName + "_fastqc.html")}



		if self.read_library_type == "pe-mp":
			return {'out1': luigi.LocalTarget(pe_readQC_folder + self.sampleName + "_R1_fastqc.html"),
					'out2': luigi.LocalTarget(pe_readQC_folder + self.sampleName + "_R2_fastqc.html"),
					'out3': luigi.LocalTarget(mp_readQC_folder + self.sampleName + "_R1_fastqc.html"),
					'out4': luigi.LocalTarget(mp_readQC_folder + self.sampleName + "_R2_fastqc.html")}
		if self.read_library_type == "pe-lr":
			return {'out1': luigi.LocalTarget(pe_readQC_folder + self.sampleName + "_R1_fastqc.html"),
					'out2': luigi.LocalTarget(pe_readQC_folder + self.sampleName + "_R2_fastqc.html"),
					'out3': luigi.LocalTarget(lr_readQC_folder + self.sampleName + "_nanoQC.html")}

		if self.read_library_type == "pe-mp-lr":
			return {'out1': luigi.LocalTarget(pe_readQC_folder + self.sampleName + "_R1_fastqc.html"),
					'out2': luigi.LocalTarget(pe_readQC_folder + self.sampleName + "_R2_fastqc.html"),
					'out3': luigi.LocalTarget(mp_readQC_folder + self.sampleName + "_R1_fastqc.html"),
					'out4': luigi.LocalTarget(mp_readQC_folder + self.sampleName + "_R2_fastqc.html"),
					'out5': luigi.LocalTarget(lr_readQC_folder + self.sampleName + "_nanoQC.html")}

		if self.read_library_type == "pe-mp-se":
			return {'out1': luigi.LocalTarget(pe_readQC_folder + self.sampleName + "_R1_fastqc.html"),
					'out2': luigi.LocalTarget(pe_readQC_folder + self.sampleName + "_R2_fastqc.html"),
					'out3': luigi.LocalTarget(mp_readQC_folder + self.sampleName + "_R1_fastqc.html"),
					'out4': luigi.LocalTarget(mp_readQC_folder + self.sampleName + "_R2_fastqc.html"),
					'out5': luigi.LocalTarget(se_readQC_folder + self.sampleName + "_fastqc.html")}

	def run(self):
		se_readQC_folder = os.path.join(os.getcwd(), "ReadQC", "PostQC-SE-Reads" + "/")
		pe_readQC_folder = os.path.join(os.getcwd(), "ReadQC", "PostQC-PE-Reads" + "/")
		mp_readQC_folder = os.path.join(os.getcwd(), "ReadQC", "PostQC-MP-Reads" + "/")
		lr_readQC_folder = os.path.join(os.getcwd(), "ReadQC", "PostQC-LONG-Reads" + "/")

		se_clean_read_folder = os.path.join(os.getcwd(), "CleanedReads" , "Cleaned_SE_Reads" + "/")
		pe_clean_read_folder = os.path.join(os.getcwd(), "CleanedReads" , "Cleaned_PE_Reads" + "/")
		mp_clean_read_folder = os.path.join(os.getcwd(), "CleanedReads" , "Cleaned_MP_Reads" + "/")
		lr_clean_read_folder = os.path.join(os.getcwd(), "CleanedReads" , "Cleaned_LONG_Reads" + "/")

		read_QC_log_folder = os.path.join(os.getcwd(), "log", "ReadQC" + "/")



		cmd_raw_pe_qc = "[ -d  {pe_readQC_folder} ] || mkdir -p {pe_readQC_folder}; mkdir -p {read_QC_log_folder}; " \
					   "/usr/bin/time -v fastqc " \
						"-t {cpu} " \
						"{pe_clean_read_folder}{sampleName}_R1.fastq " \
						"{pe_clean_read_folder}{sampleName}_R2.fastq " \
						"-o {pe_readQC_folder} " \
						"2>&1 | tee  {read_QC_log_folder}{sampleName}_cleaned_pe_fastqc.log".format(

													   sampleName=self.sampleName,
													   pe_readQC_folder=pe_readQC_folder,
													   cpu=GlobalParameter().threads,
													   pe_clean_read_folder=pe_clean_read_folder,
													   read_QC_log_folder=read_QC_log_folder)

		cmd_raw_mp_qc = "[ -d  {mp_readQC_folder} ] || mkdir -p {mp_readQC_folder};  mkdir -p {read_QC_log_folder}; " \
						"fastqc " \
						"-t {cpu} " \
						"{mp_clean_read_folder}{sampleName}_R1.fastq " \
						"{mp_clean_read_folder}{sampleName}_R2.fastq " \
						"-o {mp_readQC_folder} " \
						"2>&1 | tee  {read_QC_log_folder}{sampleName}_cleaned_mp_fastqc.log".format(
													   sampleName=self.sampleName,
													   mp_readQC_folder=mp_readQC_folder,
													   cpu=GlobalParameter().threads,
													   read_QC_log_folder=read_QC_log_folder,
													   mp_clean_read_folder=mp_clean_read_folder)

		cmd_raw_se_qc = "[ -d  {se_readQC_folder} ] || mkdir -p {se_readQC_folder};   mkdir -p {read_QC_log_folder}; " \
						"fastqc " \
						"--threads {cpu} " \
						"{se_clean_read_folder}{sampleName}.fastq " \
						"-o {se_readQC_folder} " \
						"2>&1 | tee  {read_QC_log_folder}{sampleName}_cleaned_se_fastqc.log".format(
													   sampleName=self.sampleName,
													   se_readQC_folder=se_readQC_folder,
													   cpu=GlobalParameter().threads,
													   read_QC_log_folder=read_QC_log_folder,
													   se_clean_read_folder=se_clean_read_folder)

		cmd_raw_lr_qc = "[ -d  {lr_readQC_folder} ] || mkdir -p {lr_readQC_folder};  mkdir -p {read_QC_log_folder}; " \
						"nanoQC -o {lr_readQC_folder} " \
						"{lr_clean_read_folder}{sampleName}.fastq " \
						"2>&1 | tee  {read_QC_log_folder}{sampleName}_cleaned_lr_nanoqc.log".format(sampleName=self.sampleName,
													   lr_readQC_folder=lr_readQC_folder,
													   read_QC_log_folder=read_QC_log_folder,
													   lr_clean_read_folder=lr_clean_read_folder)

		cmd_mv_lr_qc = "cd {lr_readQC_folder};  " \
						"mv nanoQC.html {sampleName}_nanoQC.html ".format(sampleName=self.sampleName,
													   lr_readQC_folder=lr_readQC_folder)

		if self.read_library_type == "se":
			print("****** NOW RUNNING COMMAND ******: " + cmd_raw_se_qc)
			print (run_cmd(cmd_raw_se_qc))

		if self.read_library_type == "pe":
			print("****** NOW RUNNING COMMAND ******: " + cmd_raw_pe_qc)
			print (run_cmd(cmd_raw_pe_qc))

		if self.read_library_type == "mp":
			print("****** NOW RUNNING COMMAND ******: " + cmd_raw_mp_qc)
			print (run_cmd(cmd_raw_mp_qc))

		if self.read_library_type == "lr":
			print("****** NOW RUNNING COMMAND ******: " + cmd_raw_lr_qc)
			print (run_cmd(cmd_raw_lr_qc))
			print("****** NOW RUNNING COMMAND ******: " + cmd_mv_lr_qc)
			print(run_cmd(cmd_mv_lr_qc))

		if self.read_library_type == "pe-mp" :
			print("****** NOW RUNNING COMMAND ******: " + cmd_raw_pe_qc)
			print(run_cmd(cmd_raw_pe_qc))
			print("****** NOW RUNNING COMMAND ******: " + cmd_raw_mp_qc)
			print(run_cmd(cmd_raw_mp_qc))
		if self.read_library_type == "pe-mp-se":
			print("****** NOW RUNNING COMMAND ******: " + cmd_raw_pe_qc)
			print(run_cmd(cmd_raw_pe_qc))
			print("****** NOW RUNNING COMMAND ******: " + cmd_raw_mp_qc)
			print(run_cmd(cmd_raw_mp_qc))
			print("****** NOW RUNNING COMMAND ******: " + cmd_raw_se_qc)
			print(run_cmd(cmd_raw_se_qc))

		if self.read_library_type == "pe-mp-lr":
			print("****** NOW RUNNING COMMAND ******: " + cmd_raw_pe_qc)
			print(run_cmd(cmd_raw_pe_qc))
			print("****** NOW RUNNING COMMAND ******: " + cmd_raw_mp_qc)
			print(run_cmd(cmd_raw_mp_qc))
			print("****** NOW RUNNING COMMAND ******: " + cmd_raw_lr_qc)
			print(run_cmd(cmd_raw_lr_qc))
			print("****** NOW RUNNING COMMAND ******: " + cmd_mv_lr_qc)
			print(run_cmd(cmd_mv_lr_qc))

		if self.read_library_type == "pe-lr":
			print("****** NOW RUNNING COMMAND ******: " + cmd_raw_pe_qc)
			print(run_cmd(cmd_raw_pe_qc))
			print("****** NOW RUNNING COMMAND ******: " + cmd_raw_lr_qc)
			print(run_cmd(cmd_raw_lr_qc))
			print("****** NOW RUNNING COMMAND ******: " + cmd_mv_lr_qc)
			print(run_cmd(cmd_mv_lr_qc))





class cleanReadsQC(luigi.Task):
	read_library_type = luigi.ChoiceParameter(choices=["lr", "se", "pe", "mp", "pe-mp", "pe-mp-se", "pe-lr", "pe-mp-lr" ],
									var_type=str)

	def requires(self):

		if self.read_library_type == "pe":
			return [postreadqc(read_library_type=self.read_library_type,
						sampleName=i)
					for i in [line.strip()
							  for line in
							  open((os.path.join(os.getcwd(), "sample_list", "pe_samples.lst")))]]

		if self.read_library_type == "lr":
			return [postreadqc(read_library_type=self.read_library_type,
						sampleName=i)
					for i in [line.strip()
							  for line in
							  open((os.path.join(os.getcwd(), "sample_list", "lr_samples.lst")))]]


		if self.read_library_type == "se":
			return [postreadqc(read_library_type=self.read_library_type,
						sampleName=i)
					for i in [line.strip()
							  for line in
							  open((os.path.join(os.getcwd(), "sample_list", "se_samples.lst")))]]


		if self.read_library_type == "mp":
			return [postreadqc(read_library_type=self.read_library_type,
						   sampleName=i)
					for i in [line.strip()
							  for line in
							  open((os.path.join(os.getcwd(),"sample_list", "mp_samples.lst")))]]


		if self.read_library_type == "pe-mp":

			return [
						[postreadqc(read_library_type="pe",sampleName=i)
								for i in [line.strip()
										  for line in
												open((os.path.join(os.getcwd(), "sample_list","pe_samples.lst")))]],

						[postreadqc(read_library_type="mp", sampleName=i)
								for i in [line.strip()
										  for line in
												open((os.path.join(os.getcwd(), "sample_list","mp_samples.lst")))]]
				  ]


		if self.read_library_type == "pe-lr":

			return [
						[postreadqc(read_library_type="pe",sampleName=i)
								for i in [line.strip()
										  for line in
												open((os.path.join(os.getcwd(), "sample_list","pe_samples.lst")))]],

						[postreadqc(read_library_type="lr", sampleName=i)
								for i in [line.strip()
										  for line in
												open((os.path.join(os.getcwd(), "sample_list","lr_samples.lst")))]]
				  ]

		if self.read_library_type == "pe-mp-se":

			return [
						[postreadqc(read_library_type="pe",sampleName=i)
								for i in [line.strip()
										  for line in
												open((os.path.join(os.getcwd(), "sample_list","pe_samples.lst")))]],

						[postreadqc(read_library_type="mp", sampleName=i)
								for i in [line.strip()
										  for line in
												open((os.path.join(os.getcwd(), "sample_list","mp_samples.lst")))]],

						[postreadqc(read_library_type="se", sampleName=i)
								for i in [line.strip()
						   				  for line in
						   						open((os.path.join(os.getcwd(), "sample_list", "se_samples.lst")))]]
				  		]
		if self.read_library_type == "pe-mp-lr":

			return [
						[postreadqc(read_library_type="pe",sampleName=i)
								for i in [line.strip()
										  for line in
												open((os.path.join(os.getcwd(), "sample_list","pe_samples.lst")))]],

						[postreadqc(read_library_type="mp", sampleName=i)
								for i in [line.strip()
										  for line in
												open((os.path.join(os.getcwd(), "sample_list","mp_samples.lst")))]],

						[postreadqc(read_library_type="lr", sampleName=i)
								for i in [line.strip()
						   				  for line in
						   						open((os.path.join(os.getcwd(), "sample_list", "lr_samples.lst")))]]
				  		]




	def output(self):
		timestamp = time.strftime('%Y%m%d.%H%M%S', time.localtime())
		return luigi.LocalTarget(os.path.join(os.getcwd(),"task_logs",'task.clean.read.qc.analysis.complete.{t}'.format(
			t=timestamp)))

	def run(self):
		timestamp = time.strftime('%Y%m%d.%H%M%S', time.localtime())
		with self.output().open('w') as outfile:
			outfile.write('Cleaned Read QC Assessment finished at {t}'.format(t=timestamp))










		





