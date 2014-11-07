/* 
 * Copyright 2013 Alec Ten Harmsel
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

ngrams = LOAD '$INPUT' AS (ngram:chararray, year:int, match_count:long, volume_count:long);
ngramsGroup = GROUP ngrams BY ngram;
counts = FOREACH ngramsGroup GENERATE ngrams.ngram, SUM(ngrams.match_count);
counts = FOREACH counts GENERATE FLATTEN($0), $1;
counts = DISTINCT counts;
STORE counts INTO '$OUTPUT/counts';
countsGroup = GROUP counts ALL;
max = FOREACH countsGroup GENERATE MAX(counts.$1);
wholeMax = FILTER counts BY $1 == (long)max.$0;
STORE wholeMax INTO '$OUTPUT/max';
second = FILTER counts BY $1 < (long)max.$0;
secondGroup = GROUP second ALL;
secondMax = FOREACH secondGroup GENERATE MAX(second.$1);
wholeSecond = FILTER second BY $1 == (long)secondMax.$0;
STORE wholeSecond INTO '$OUTPUT/second';
/* Recursive-safe */
second = FILTER counts BY $1 < (long)secondMax.$0;
secondGroup = GROUP second ALL;
secondMax = FOREACH secondGroup GENERATE MAX(second.$1);
wholeSecond = FILTER second BY $1 == (long)secondMax.$0;
STORE wholeSecond INTO '$OUTPUT/third';
second = FILTER counts BY $1 < (long)secondMax.$0;
secondGroup = GROUP second ALL;
secondMax = FOREACH secondGroup GENERATE MAX(second.$1);
wholeSecond = FILTER second BY $1 == (long)secondMax.$0;
STORE wholeSecond INTO '$OUTPUT/fourth';
second = FILTER counts BY $1 < (long)secondMax.$0;
secondGroup = GROUP second ALL;
secondMax = FOREACH secondGroup GENERATE MAX(second.$1);
wholeSecond = FILTER second BY $1 == (long)secondMax.$0;
STORE wholeSecond INTO '$OUTPUT/fifth';
second = FILTER counts BY $1 < (long)secondMax.$0;
secondGroup = GROUP second ALL;
secondMax = FOREACH secondGroup GENERATE MAX(second.$1);
wholeSecond = FILTER second BY $1 == (long)secondMax.$0;
STORE wholeSecond INTO '$OUTPUT/sixth';
second = FILTER counts BY $1 < (long)secondMax.$0;
secondGroup = GROUP second ALL;
secondMax = FOREACH secondGroup GENERATE MAX(second.$1);
wholeSecond = FILTER second BY $1 == (long)secondMax.$0;
STORE wholeSecond INTO '$OUTPUT/seventh';
second = FILTER counts BY $1 < (long)secondMax.$0;
secondGroup = GROUP second ALL;
secondMax = FOREACH secondGroup GENERATE MAX(second.$1);
wholeSecond = FILTER second BY $1 == (long)secondMax.$0;
STORE wholeSecond INTO '$OUTPUT/eighth';
second = FILTER counts BY $1 < (long)secondMax.$0;
secondGroup = GROUP second ALL;
secondMax = FOREACH secondGroup GENERATE MAX(second.$1);
wholeSecond = FILTER second BY $1 == (long)secondMax.$0;
STORE wholeSecond INTO '$OUTPUT/ninth';
second = FILTER counts BY $1 < (long)secondMax.$0;
secondGroup = GROUP second ALL;
secondMax = FOREACH secondGroup GENERATE MAX(second.$1);
wholeSecond = FILTER second BY $1 == (long)secondMax.$0;
STORE wholeSecond INTO '$OUTPUT/tenth';
