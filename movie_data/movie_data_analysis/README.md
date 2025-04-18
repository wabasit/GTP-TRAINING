# TMDB Movie Data Analysis using Pandas and APIs

---

This project focuses on cleaning and preprocessing a movie dataset to prepare it for further analysis or modeling. The dataset contains numerical and categorical attributes such as `budget`, `revenue`, `runtime`, and `genres`.

---

## Objectives

- Replace unrealistic values (e.g., `0` in `budget`, `revenue`, `runtime`) with `NaN`.
- Convert financial columns to represent values in **millions of dollars**.
- Extract meaningful data from nested or complex formats (e.g., extracting genre names from dictionaries/lists).
- Drop duplicate records for better data integrity.

---

## Steps Performed

### 1. Replacing Unrealistic Values
Replaced zeros in `budget`, `revenue`, and `runtime` columns with `NaN` to reflect missing or invalid data.

```python
df[["budget", "revenue", "runtime"]] = df[["budget", "revenue", "runtime"]].replace(0, np.nan)
```

dataset/
    └── movie_data.csv
movie_data_analysis/
    ├── __init__.py
    ├── data_clean.py
    └── final_dataframe.py
    └── final_dataframe.py
├── cleaned_movie.csv
├── raw_movie_data.csv
├── README.md
└── requirements.txt

---

## Steps

- Perform feature engineering

- Handle missing values

- Visualization and EDA

---

## Requirements

```
- pip install -r requirements.txt
```

##  Run it from the parent directory
```
python -m movie_data_analysis.final_dataframe
```


