"""
Find all numbers that appear at least three times consecutively.

Return the result table in any order.

The result format is in the following example.

 

Example 1:

Input: 
Logs table:
+----+-----+
| id | num |
+----+-----+
| 1  | 1   |
| 2  | 1   |
| 3  | 1   |
| 4  | 2   |
| 5  | 1   |
| 6  | 2   |
| 7  | 2   |
+----+-----+
Output: 
+-----------------+
| ConsecutiveNums |
+-----------------+
| 1               |
+-----------------+
Explanation: 1 is the only number that appears consecutively for at least three times.
"""
{# +-------------+---------------+ #}
{# Explanation: The average experience years for the first project #}

select 
    x, 
    y,
    z,
    case
        when x + y > z 
        and y + z > x
        and x + z > 
            then 'Yes' else 'No' end
from Triangle