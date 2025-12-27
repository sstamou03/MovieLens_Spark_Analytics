# MovieLens Analytics with Spark & Scala

## Project Overview

This project uses **Apache Spark** and **Scala** to perform **Big Data analytics** on the full **MovieLens dataset**, including:

- Movies  
- Ratings  
- Tags  
- Genome Scores  

The goal is to build a **functional programming application** that processes large-scale data and extracts meaningful insights about **user preferences** and **movie characteristics**.

The project is divided into two main parts:

- **Part A:** Low-level data processing using **Spark RDDs**
- **Part B:** High-level structured analytics using **Spark DataFrames**

---

## My Contribution (Even Queries)

I was responsible for the **even-numbered analytics scenarios**:  
**Queries 2, 4, 6, 8, and 10**.

My work focused on:
- Advanced matching algorithms  
- Multi-dimensional filtering  
- Semantic similarity and skyline queries  

---

## Part A: RDD Analytics

### Query 2: Tag Dominance per Genre

**Description**  
Identifies the most frequently used *tag* for each movie genre (e.g., `Action → "explosions"`) and calculates the **average rating** of movies associated with that tag.

**Purpose**  
To understand the vocabulary users commonly use to describe different genres.

---

### Query 4: Tag Sentiment Analysis

**Description**  
Calculates the **average rating** for every unique user-assigned tag.

**Purpose**  
To determine which tags imply positive sentiment (e.g., *"masterpiece"*) versus negative sentiment (e.g., *"boring"*).

---

## Part B: DataFrame Analytics

### Query 6: Skyline Query (Finding "Perfect Balance" Movies)

**Description**  
Identifies movies that are **not dominated** by any other movie.  
A movie is retained only if no other movie outperforms it in **all three dimensions**:

- Average Rating  
- Popularity (number of ratings)  
- Relevance (genome score relevance)

**Purpose**  
To highlight top-performing movies without relying on a single metric.

---

### Query 8: Reverse User Matching

**Description**  
Given a **target movie**, this query identifies users who are most likely to enjoy it.  
It compares the movie’s **genome vector** with user profiles built from their highly rated movies using **Cosine Similarity**.

**Purpose**  
To answer the question:  
> *“Who should we market this movie to?”*

---

### Query 10: Top-K Semantic Search

**Description**  
An extension of Query 8 that ranks users based on similarity and returns the **Top-K users** whose tastes best match the target movie’s semantic profile.

**Purpose**  
Useful for:
- Targeted recommendations  
- Early screening invitations  
- Personalized marketing strategies  

---

## Technologies Used

- **Language:** Scala  
- **Framework:** Apache Spark (RDDs & DataFrames)  
- **Cluster:** SoftNet Hadoop Cluster (YARN)  
- **Dataset:** MovieLens Latest (Full Version)

---

## How to Run

### Build
```bash
sbt package
