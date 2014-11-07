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
ngramsGroup = GROUP ngrams BY year;
max = FOREACH ngramsGroup GENERATE FLATTEN(ngrams.year), MAX(ngrams.match_count);
out = JOIN ngrams BY year FULL OUTER, max BY $0; 
out = FILTER out BY ngrams::match_count == $5; 
out = DISTINCT out;
out = FOREACH out GENERATE ngrams::year, ngrams::ngram, ngrams::match_count;
STORE out INTO '$OUTPUT/yearlyMax';
