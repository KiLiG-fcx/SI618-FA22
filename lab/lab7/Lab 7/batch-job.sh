#!/bin/bash
#SBATCH --job-name=spark-cluster-si618-f22-hw6
#SBATCH --account=siads618f22_class        
#SBATCH --partition=standard
#SBATCH --nodes=2                
#SBATCH --ntasks-per-node=1      
#SBATCH --cpus-per-task=4       
#SBATCH --mem=16g               
#SBATCH --time=00:30:00
#SBATCH --mail-type=NONE

# These modules are required. You may need to customize the module version
# depending on which cluster you are on.
module load spark/3.2.1 python/3.10.4 pyarrow/8.0.0

# Start the Spark instance.
spark-start


# Source spark-env.sh to get useful env variables.
source ${HOME}/.spark-local/${SLURM_JOB_ID}/spark/conf/spark-env.sh

# Customize the executor resources below to match resources requested above
# with an allowance for spark driver overhead. Also change the path to your spark job.
spark-submit --master ${SPARK_MASTER_URL} \
  --executor-cores 2 \
  --executor-memory 2G \
  --total-executor-cores 16 \
  uniqname_si618_lab7_solution.py 
