SELECT MIN(t.title) AS movie_title
FROM keyword AS k,
     movie_info AS mi,
     movie_keyword AS mk,
     title AS t
WHERE k.keyword LIKE '%sequel%'
  AND t.id = mi.movie_id
  AND t.id = mk.movie_id
  AND mk.movie_id = mi.movie_id
  AND k.id = mk.keyword_id
  AND mi.info IN ('Swedish',
'Sweden',
'English',
'Denish',
'America',
'Norwegian',
'USA',
'Germany',
'Danish',
'Denmark')
AND t.production_year > 1998;