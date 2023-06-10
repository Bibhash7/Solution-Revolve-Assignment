
1.WITH CTE AS(
SELECT MANUFACTURER, COUNT(TAILNUM) AS NO_OF_FLIGHTS
FROM PLANES
GROUP BY manufacturer
ORDER BY COUNT(TAILNUM) DESC
)

SELECT MANUFACTURER, NO_OF_FLIGHTS
FROM CTE 
WHERE NO_OF_FLIGHTS IN (SELECT NO_OF_FLIGHTS FROM CTE LIMIT 1)

2. WITH CTE AS(
SELECT P.manufacturer, max(CAST(F.HOUR AS INTEGER)*60+CAST(F.MINUTE AS INTEGER)) AS MAX_TIME
FROM PLANES P JOIN FLIGHTS F ON P.TAILNUM = F.TAILNUM
GROUP BY P.manufacturer
ORDER BY max(CAST(F.HOUR AS INTEGER)*60+CAST(F.MINUTE AS INTEGER)) DESC
)

SELECT MANUFACTURER, MAX_TIME/60 AS HOUR, MAX_TIME%60 AS MINUTE
FROM CTE
WHERE MAX_TIME IN (SELECT MAX_TIME FROM CTE LIMIT 1);

3. WITH CTE AS (
SELECT TAILNUM, SUM(CAST(DEP_TIME AS INT)-CAST(DEP_DELAY AS INT)+ CAST(ARR_TIME AS INT)+CAST(ARR_DELAY AS INT)) AS FLYING_HOURS
FROM FLIGHTS
GROUP BY TAILNUM
ORDER BY SUM(CAST(DEP_TIME AS INT)-CAST(DEP_DELAY AS INT)+ CAST(ARR_TIME AS INT)+CAST(ARR_DELAY AS INT)) DESC
)

SELECT TAILNUM, FLYING_HOURS/60 AS FLYING_HOURS
FROM CTE WHERE FLYING_HOURS IN (SELECT FLYING_HOURS FROM CTE LIMIT 1)

4. WITH CTE AS(
SELECT DEST, MAX(CAST(ARR_DELAY AS INTEGER)+CAST(DEP_DELAY AS INTEGER)) AS MAX_DELAY
FROM FLIGHTS
GROUP BY DEST
ORDER BY MAX(CAST(ARR_DELAY AS INTEGER)+CAST(DEP_DELAY AS INTEGER)) DESC
LIMIT 1
)
SELECT DEST FROM CTE;

5. WITH CTE AS(
SELECT P.manufacturer, MAX(DISTANCE) AS DISTANCE
FROM PLANES P JOIN FLIGHTS F ON P.TAILNUM = F.TAILNUM
GROUP BY P.manufacturer
ORDER BY MAX(DISTANCE) DESC
)

SELECT MANUFACTURER, DISTANCE 
FROM CTE WHERE DISTANCE IN (SELECT DISTANCE FROM CTE LIMIT 1);

6. 