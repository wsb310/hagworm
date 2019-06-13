OriginFile=$1/$2.log
TargetFile=$1/$2.$(date +%y%m%d.%H%M%S).log

if [ -f $OriginFile ]; then
	mv $OriginFile $TargetFile
	touch $OriginFile
else
	touch $OriginFile
fi
