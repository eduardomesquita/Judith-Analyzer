$(ps aux | grep storm | awk '{print $2}' | xargs kill -9)
#mvn exec:java -Dexec.mainClass="br.judith.storm.main.Main"
