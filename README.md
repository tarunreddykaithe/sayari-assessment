# **Sayari Labs Spark Task - Sanctioned Entities**

## **Environment Setup**

### Dependencies:

    1. JAVA>=1.8
    2. PYTHON>3.6

### **Initial environment setup**

Below commands to help you setup environment:

To create a virtual environment

```
python3 -m venv ./venv
```

To activate the virtual environment:

```
source venv/bin/activate
```

To install required libraries for this project:

```
pip3 install -r requirements.txt
```

### Command to execute the task

```
 spark-submit --master local[1] sanctioned_entities.py
```

## Result

For review, results are added as `results.csv` file.

The output file contains below fields:

1. ofac_id - id of OFAC record
2. gbr_id - id of UK Treasury lists
3. Type - Type of entity i.e Entity, Individual..
4. Name - Name of entity
5. name_matches - True if name matches
6. atleast_1_date_of_births_matches - True if atleast one date of birth match
7. atleast_1_aliases_matches - True if atleast one alias match
8. atleast_1_id_numbers_matches - True if atleast one id_numbers match

### challenges
