lazy
====

yet another log analyzer

```
%s/topics/%s
```

return topic

`lazy/topic/syslog`
```
{"log_type":"rfc3164","split_regexp":"\(|\)|{|}|/","log_source":"onlinesystemlog","index_ttl":"604800","addtion_check":["regexp","bayes"]}
```

```
%s/regexp/%s
```
return regexp for topic
`lazy/regexp/syslog/sudo`
```
[{"exp":"asd","ttl":"30"}]
```

```
%s/classifiers/%s
```
return classifiers for topic
`lazy/classifiers/syslog/normal`
```
a,b,c
```
