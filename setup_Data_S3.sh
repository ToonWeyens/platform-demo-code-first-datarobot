#aws sso login --profile datarobotcfds

s3DataDir="toon.weyens/Orsted/"
echo "Files in folder ${s3DataDir}:"
aws --profile datarobotcfds s3 ls s3://public-demo-datasets/${s3DataDir}
if [ $? -ne 0 ] ; then echo "cannot list data directory" && exit 1 ; fi

read -p "Copy data files to s3? [Yy/Nn]" -n 1 -r
echo    # (optional) move to a new line
if [[ $REPLY =~ ^[Yy]$ ]]
then
    echo "copying ./Data/AFD_qc_1_b_ts.csv"
    aws --profile datarobotcfds s3 cp ./Data/AFD_qc_1_b_ts.csv s3://public-demo-datasets/toon.weyens/Orsted/
    echo "copying ./Data/AFD_qc_2_ts.csv"
    aws --profile datarobotcfds s3 cp ./Data/AFD_qc_2_ts.csv s3://public-demo-datasets/toon.weyens/Orsted/
fi
