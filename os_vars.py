import subprocess

exp = 'setx POSTGRES_USER "postgres"'
subprocess.Popen(exp, shell=True).wait()
exp = 'setx POSTGRES_PASSWORD "1314151617"'
subprocess.Popen(exp, shell=True).wait()
exp = 'setx POSTGRES_DB "sample_db2022_lab1"'
subprocess.Popen(exp, shell=True).wait()