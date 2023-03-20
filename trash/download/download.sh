# This scirpt is to download the files to the local directory

# make sure the credential is correctly configured

source_path=s3://quokka-2021-thesis/pubmed21-part/
target_path=$1

if [ ! $target_path ]
then
    target_path='.'
elif [ ! -d $target_path ]
then
    mkdir $target_path
fi

aws s3 cp $source_path $target_path --recursive
    