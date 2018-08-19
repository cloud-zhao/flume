#!/usr/bin/env bash
. /etc/profile
. $HOME/.bash_profile

base=$(cd $(dirname $0);pwd)

lib_dir="$base/lib"
head_size="512m"
main="z.cloud.canal.ClientSimple"

jars=""
for i in $(ls $lib_dir)
do
	jars=$(echo $i | awk -v l="$lib_dir" '{if($0~/.*jar$/){print l"/"$0}}'):$jars
done
classpath=$CLASSPATH
test "x$jars" != "x" && classpath=$CLASSPATH:${jars%:}



echo "java -Xmx$head_size -Xms$head_size -cp $classpath $main >$base/out.log 2>&1 &"
java -Xmx$head_size -Xms$head_size -cp $classpath $main >$base/out.log 2>&1 &
