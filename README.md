cobar
=====

Based on alibaba cobar 1.2.6 modifications.

* table name add regex support，and regex must contain "[]",then remove "," split tables
  (The regular expression contained in the table can not be repeated.)；
* rule add divide by ID Mod;
* startup params modify Xss128k to Xss256k；
