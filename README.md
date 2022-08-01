# How to run
```
screen
```
```
HADOOP_USER_NAME=sl90meco \
srun --export=ALL --pty \
--mem=50g \
--container-name=classification-pipeline \
--container-image=ghcr.io/jonaskonig/classification_pipeline:master \
--container-mounts=/mnt/ceph:/mnt/ceph --container-writable --gres=gpu:ampere:1 \
bash -c " cd /mnt/ceph/storage/data-tmp/teaching-current/sl90meco/classification_pipeline \
&& PYTHONPATH=. HADOOP_CONF_DIR="./hadoop/" \
python3 main.py"
```

Alternatively:

````
srun --mem=50g enroot import --output classification-pipeline-image.sqsh docker://ghcr.io/jonaskonig/classification_pipeline:master
```
```
HADOOP_USER_NAME=sl90meco \
srun --export=ALL --pty \
--mem=75g \
--container-name=classification-pipeline \
--container-image=./classification-pipeline-image.sqsh \
--container-mounts=/mnt/ceph:/mnt/ceph --container-writable --gres=gpu:ampere:1 \
bash -c " cd /mnt/ceph/storage/data-tmp/teaching-current/sl90meco/classification_pipeline \
&& PYTHONPATH=. HADOOP_CONF_DIR="./hadoop/" \
python3 main.py"
```