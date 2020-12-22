#ES-SQL

支持select,delete by query,update by query,insert,show。<br>
其中delete by query,update by query无法explain。

##sql的jdbc

jdbc支持commit,rollback,batch,大字段<br>
第三方支持kettle，jdbcTemplate

目前版本只支持6.8.2和5.3.0，由于使用RestClient和RestHighLevelClient所以小版本不会有太大差别

##非jdbc
com.ngw.SqlUtil.getRestClient   获取客户端<br>
com.ngw.SqlUtil.requestSql  通过SQL请求ES<br>
com.ngw.SqlUtil.reques  t原生请求ES<br>
通过原生的请求，主要保证让ES版本等等和业务解耦。
