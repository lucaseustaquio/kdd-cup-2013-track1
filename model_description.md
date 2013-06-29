Model description.

Basic algorithm: GBM (package gbm in R)
Parameters:
number of trees = 4500
interaction depth = 2
shrinkage = 0.06
distribution = bernoulli
bag.fraction = 0.5
train.fraction = 1.0

Features classification:
1) Author features - calculated just for authors (name starts from af_)
2) Paper features - calculated just for papers (name starts from pf_)
3) Author-Paper features - calculated for pairs author-paper (name starts from apf_)

List of features with description:
Notice. All features are calculated based on cleaned PaperAuthor table: 
for each author in Train, Valid and Test sets, pair author-paper has been removed from PaperAuthor table 
if paper is not in Train, Valid or Test set.

1) Author features.
- af_countcoauthors: count all coauthors of author
- af_countjournals: count different journals where author published 
- af_countconferences: count different conferences where author published 
- af_countpapers: count different papers of author
- af_sumismissing: number of times pair author-paper is missing amongst all author papers (author+duplicated authors here)
- af_nauthorsrep: number of times an author is repeated (sum for distinct ids)
- af_ismaxauthorid: current authorid is the highest authorid for all author duplicates
- af_isminauthorid: current authorid is the lowest authorid for all author duplicates
- af_countkeywords: count keywords of author
- af_countdiffkeywords: count distinct keywords of author
- af_tfidf: weighted tf-idf measure of all author keywords inside author's papers
- af_tfidfbypapers: weighted tf-idf measure of all author keywords inside all papers
- af_tfidfbypapers_def: tf_idf measure of all author keywords inside all papers
- af_confratio: discretized ratio of number of papers in correct conferences to all number of papers
- af_journalratio: same as previous, but for journals
 
2) Paper features.
- pf_year: year of paper
- pf_type: 0 - if conferenceid>0, 1 - if journalid>0, 2 - if both>0, -1 - if none>0
- pf_correctkeywords: binary feature, checking if field keyword is not empty
- pf_badconference: binary feature, checking if conferenceid == -1
- pf_countauthors: count authors of paper
- pf_countauthors_extraclean: count authors of paper without duplicates
- pf_journalcountpapers: count papers in the same journal
- pf_journalcountauthors: count authors in the same journal
- pf_conferencecountpapers: count papers in the same conference
- pf_conferencecountauthors: count authors in the same conference
- pf_npapersrep: count duplicated papers for paper
- pf_isminpaperid: current paperid is the lowest paperid for all paper duplicates
- pf_nuniqueauthors: number of distinct authorid duplicates
- pf_nauthorsrep: count duplicated authors for paper
- pf_nuniqueauthorsrep: number of authorid duplicates
- pf_nauthorveryhiddensources: number of sources for all duplicated authorid x duplicated paperid combinations
- pf_authorsprop: pf_countauthors / pf_nauthorsrep
- pf_countkeywords: count keywords in paper
- pf_tfidfbyjournal: weighted tfidf metric of keywords inside the journal
- pf_tfidfbyconference: weighted tfidf metric of keywords inside the conference
- pf_tfidfbyall: weighted tfidf metric of keywords inside whole dataset
- pf_countsameset: how many time the exact same set of authors is repeated in different papers (without duplicates)

3) Author-Paper features.
- apf_correctaffiliation: correct affiliation from the table PaperAuthor: binary feature
- apf_year: which year the author publishes (for the first year papers of author this feature equals 1, for the second year papers - 2 and so on)
- apf_source: count sources: number of times pair author-paper is appeared in the table PaperAuthor
- apf_sourceaff: count sources by affiliation: number of times pair author-paper with non-empty affiliation field is apperated in he table PaperAuthor
- apf_sourceaff_extraclean: the same as previous but for PaperAuthor table with removed duplicates
- apf_hiddensource_unique: number of distinct repeated words in the paper from author name field (this feature is calculated inside each paper)
- apf_hiddensource: number of repeated words in the paper from author name field (this feature is calculated inside each paper)
- apf_sameauthortarget: for each author we find repeated author (by name) and check if this repeated author has known target, the value of features is this target
- apf_commonaffauthors: rate of authors with the same affiliation (inside one paper)
- apf_commonaffwords: rate of common affiliation words (inside one paper)
- apf_authorjournalcount: number of papers of the same author in journalid for paper
- apf_authorjournalcount_keyword: number of papers with non-empty keywords of the same author in journalid for paper
- apf_authorjournalcount_year: number of papers with correct years of the same author in journalid for paper
- apf_authorjournalcount_object: number of papers with correct journalid or conferenceid of the same author in journalid for paper
- apf_authorconferencecount: number of papers of the same author in conferenceid for paper
- apf_authorconferencecount_title: number of papers with non-empty titles of the same author in conferenceid for paper
- apf_authorconferencecount_year: number of papers with correct years of the same author in conferenceid for paper
- apf_authorconferencecount_object: number of papers with correct journalid or conferenceid of the same author in conferenceid for paper
- apf_conferenceaffiliationcount: count number of papers of the same affiliation authors in the same conference
- apf_countpaperstogether: count papers with coauthors
- apf_countpaperstogether_nosameauthors: count papers with coauthors without repeated authors inside paper
- apf_countpaperstogether_keyword: count papers with non-empty keywords with coauthors
- apf_countpaperstogether_extraclean: count papers with coauthors (with removed repeated papers: papers are the same if title is the same)
- apf_countkeywordstogether: count keywords together
- apf_commonjournalnameword: count common journal name word between all coauthors' journals
- apf_commonjournalwebword: count common journal web word between all coauthors' journals
- apf_commonconferencenameword: count common conference name word between all coauthors' conferences
- apf_commonconferencewebword: count common conference web word between all coauthors' conferences
- apf_authoryearcount: count papers of author in the same year
- apf_uniquekeywords: count unique author keywords in the paper (keywords appeared only in this paper)
- apf_frequentkeywords: count frequent keywords in the paper  (keywords appeared more than 1 time)
- apf_veryfrequentkeywords: count very frequent keywords in the paper  (keywords appeared more than 3 times)
- apf_ratefrequentkeywords: apf_frequentkeywords/apf_uniquekeywords
- apf_papersameaff: count of authors with the same affiliation inside one paper
- apf_samename: same name: are names in PaperAuthor table and Author table the same
- apf_sameaffiliation: same affiliation: are affiliations in PaperAuthor table and Author table the same
- journalid_prob: log likelihood of journalid 
- conferenceid_prob: log likelihood of confenrenceid
- paperid_prob: log likelihood of paperid
- p_year_prob: likelihood of paperauthor year
- p_title_prob: likelihood of paperauthor title
- apf_fakesource: source count for fake duplicates (used file Track2_Dup_Greedy.csv)
- apf_dupsource: source count for duplicates (used file Track2_Dup.csv)
- apf_sourceratio: average apf_source for each paper
- lf_countsource: count pairs author-paper in Train, Valid and Test sets

