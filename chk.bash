
(
echo "<foo>"
#project/dependencies/dependency
xmlstarlet sel -NS "http://maven.apache.org/POM/4.0.0" \
	-t -m "/_:project/_:dependencies/_:dependency[_:scope[text()='test'] and _:groupId[text()!='org.apache.druid']]" \
	-c . -n \
	`find . -name pom.xml ! -path './testdeps/*' ! -path './extensions-contrib/*'` |
	sed -r 's/ xmlns=[^>]+//g'

echo "</foo>"
) | xmlstarlet fo  | grep -v '<scope>test' |
xmlstarlet sel -t -m '//dependency' -s A:T:L 'concat(groupId[text()],artifactId[text()])' -c . -n
#-v 'groupid'
#| xmlstarlet sel -T -t -m /foo/dependency -s groupId:artifactId:
# 1687