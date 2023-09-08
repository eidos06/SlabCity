# SlabCity Supplementary Materials
This repo presents benchmarks(LeetCode, Calcite) used by SlabCity. SlabCity is a synthesis-based query rewriting technique capable of whole-query optimization without relying on any rewrite rules. SlabCity directly searches the space of SQL queries using a novel query synthesis algorithm that leverages a new concept called query dataflows.

Due to patent copyright issues, we are not open sourcing our code. Any local copies of code obtained during evaluation phase and before 9/8/2023 should be treated condidential and should not be shared or distributed without explicit permission from the authors.

## File structure

This repo contains following supplemental materials:
* A new benchmark for query rewriting, which has 1131 real-life queries from LeetCode participants
* Calcite benchmark used in the paper



A brief structure is shown below:

```bash
├── benchmarks
│   ├── LeetCode # leetcode benchmark used in paper
│   └── CalCite # calcite benchmark used in paper
└── README.md
```

## Benchmarks
Each benchmark under `benchmarks` folder contains two sub-folders: `queries` and `schema`. Queries are stored in `csv` files in `queries` folder, where each line has a unique query id in each problem and the query text.

### Queries
Queries are stored in `csv` files in `queries` folder, where each line has a unique query id in each problem and the query text. For example, `benchmarks/LeetCode/queries/1308.csv` has a line
```
2,"select distinct gender, day, sum(score_points) over (partition by gender order by day) as total from scores order by gender, day"
```
which means this query is crawled from LeetCode problem `1308`, and a unique id `2`. Each query is syntactically different.


### Schemas
`schema` folder contains schemas stored in `json` file. The schema has following format:

```
{
    "Problem Number": ..
    "Tables":[
        {
            "TableName": ..,
            "PKeys":[..]
            "FKeys":[..]
            "Others":[..]
        }
    ]

}
```
Primary keys are recorded in `PKeys` array, Foreign keys are recorded in `FKeys` array, and other columns are recorded in `Others` array,

Besides these integrity constraints, LeetCode problems often have more constraints. A complete set of constraints can be can be found in `contraints` folder under `LeetCode` benchmark folder. 
