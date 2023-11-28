## Naming Conflicts in Module Imports

Importing modules in Python and R can lead to naming conflicts if a function with that name already exists. This article demonstrates why you should be careful when importing modules to ensure that these conflicts do not occur.

A common example in Python is using [`from pyspark.sql.functions import *`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html), which will overwrite some built-in Python functions (e.g. `sum()`). Instead, it is good practice to use `from pyspark.sql import functions as F`, where you prefix the functions  with `F`, e.g. [`F.sum()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.sum.html).

### Naming variables

When writing code, it is important to give your variables sensible names, that are informative but not too long. A good reference on this is the [Clean Code](https://best-practice-and-impact.github.io/qa-of-code-guidance/core_programming.html#clean-code) section from [QA of Code for Analysis and Research](https://best-practice-and-impact.github.io/qa-of-code-guidance/intro.html). **You should avoid using the names of existing built in functions for user-defined variables**.

### Keywords

Some words are reserved: for instance, in Python you cannot have a variable called `def`, `False` or `lambda`. These are referred to as *keywords* and the code will not even compile if you try, raising a `SyntaxError`. You can generate a list of these with [`keyword.kwlist`](https://docs.python.org/3/library/keyword.html).

In R, use `?reserved` to get a list of the reserved words.
````{tabs}
```{code-tab} py
import keyword
print(keyword.kwlist)
```

```{code-tab} r R

?reserved

```
````

````{tabs}

```{code-tab} plaintext Python Output
['False', 'None', 'True', 'and', 'as', 'assert', 'break', 'class', 'continue', 'def', 'del', 'elif', 'else', 'except', 'finally', 'for', 'from', 'global', 'if', 'import', 'in', 'is', 'lambda', 'nonlocal', 'not', 'or', 'pass', 'raise', 'return', 'try', 'while', 'with', 'yield']
```

```{code-tab} plaintext R Output
Reserved                 package:base                  R Documentation

_R_e_s_e_r_v_e_d _W_o_r_d_s _i_n _R

_D_e_s_c_r_i_p_t_i_o_n:

     The reserved words in R's parser are

     ‘if’ ‘else’ ‘repeat’ ‘while’ ‘function’ ‘for’ ‘in’ ‘next’ ‘break’

     ‘TRUE’ ‘FALSE’ ‘NULL’ ‘Inf’ ‘NaN’ ‘NA’ ‘NA_integer_’ ‘NA_real_’
     ‘NA_complex_’ ‘NA_character_’

     ‘...’ and ‘..1’, ‘..2’ etc, which are used to refer to arguments
     passed down from a calling function, see ‘...’.

_D_e_t_a_i_l_s:

     Reserved words outside quotes are always parsed to be references
     to the objects linked to in the ‘Description’, and hence they are
     not allowed as syntactic names (see ‘make.names’).  They *are*
     allowed as non-syntactic names, e.g. inside backtick quotes.


```
````
### Built in functions and module imports in Python

<details>
    
<summary><b>Python Example</b></summary>

You might notice that the Python keyword list is quite short and that some common Python functionality is not listed, for instance, `sum()` or `round()`. This means that it is possible to overwrite these; obviously this is not good practice and should be avoided. 

This can be surprisingly easy to do in PySpark, and can be hard to debug if you do not know the reason for the error.

#### Python Example

First, look at the documentation for `sum`:
````{tabs}
```{code-tab} py
help(sum)
```
````

````{tabs}

```{code-tab} plaintext Python Output
Help on built-in function sum in module builtins:

sum(iterable, start=0, /)
    Return the sum of a 'start' value (default: 0) plus an iterable of numbers
    
    When the iterable is empty, return the start value.
    This function is intended specifically for use with numeric values and may
    reject non-numeric types.
```
````
Show that `sum` works with a simple example: adding three integers together:
````{tabs}
```{code-tab} py
sum([1, 2, 3])
```
````

````{tabs}

```{code-tab} plaintext Python Output
6
```
````
Now import the modules we need to use Spark. The recommended way to do this is `import pyspark.sql.functions as F`, which means that whenever you want to access a function from this module you prefix it with `F`, e.g. `F.sum()`. Sometimes the best way to see why something is recommended is to try a different method and show it is a bad idea, in this case, importing all the `functions` as `*`:
````{tabs}
```{code-tab} py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
```
````
Attempting to sum the integers will now give an error:
````{tabs}
```{code-tab} py
try:
    sum([1, 2, 3])
except AttributeError as e:
    print(e)
```
````

````{tabs}

```{code-tab} plaintext Python Output
'NoneType' object has no attribute '_jvm'
```
````
To see why this error exists, take another look at `help(sum)`; we can see that the documentation is different to previously.
````{tabs}
```{code-tab} py
help(sum)
```
````

````{tabs}

```{code-tab} plaintext Python Output
Help on function sum in module pyspark.sql.functions:

sum(col)
    Aggregate function: returns the sum of all values in the expression.
    
    .. versionadded:: 1.3
```
````
So by importing all the PySpark functions we have overwritten some key Python functionality. Note that this would also apply if you imported individual functions, e.g. `from pyspark.sql.functions import sum`.

You can also overwrite functions with your own variables, often unintentionally. As an example, first Start a Spark session:
````{tabs}
```{code-tab} py
spark = (SparkSession.builder.master("local[2]")
         .appName("module-imports")
         .getOrCreate())
```
````
Create a small DataFrame:
````{tabs}
```{code-tab} py
sdf = spark.range(5).withColumn("double_id", col("id") * 2)
sdf.show()
```
````

````{tabs}

```{code-tab} plaintext Python Output
+---+---------+
| id|double_id|
+---+---------+
|  0|        0|
|  1|        2|
|  2|        4|
|  3|        6|
|  4|        8|
+---+---------+
```
````
Loop through the columns, using `col` as the control variable. This will work, but is not a good idea as it is overwriting `col()` from `functions`:
````{tabs}
```{code-tab} py
for col in sdf.columns:
    sdf.select(col).show()
```
````

````{tabs}

```{code-tab} plaintext Python Output
+---+
| id|
+---+
|  0|
|  1|
|  2|
|  3|
|  4|
+---+

+---------+
|double_id|
+---------+
|        0|
|        2|
|        4|
|        6|
|        8|
+---------+
```
````
If we try adding another column with `col()` then it will not work as we have now reassigned `col` to be `double_id`:
````{tabs}
```{code-tab} py
try:
    sdf = sdf.withColumn("triple_id", col("id") * 3)
except TypeError as e:
    print(e)
```
````

````{tabs}

```{code-tab} plaintext Python Output
'str' object is not callable
```
````

````{tabs}
```{code-tab} py
col
```
````

````{tabs}

```{code-tab} plaintext Python Output
'double_id'
```
````
Importing the PySpark `functions` as `F` and using [`F.col()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.col.html) solves this problem:
````{tabs}
```{code-tab} py
from pyspark.sql import functions as F 
sdf = sdf.withColumn("triple_id", F.col("id") * 3)
sdf.show()
```
````

````{tabs}

```{code-tab} plaintext Python Output
+---+---------+---------+
| id|double_id|triple_id|
+---+---------+---------+
|  0|        0|        0|
|  1|        2|        3|
|  2|        4|        6|
|  3|        6|        9|
|  4|        8|       12|
+---+---------+---------+
```
````
</details>

### Built in functions and package imports in R

<details>
<summary><b>R Example</b></summary>

It is advised to use `::` to directly call a function from a package. For instance, there is a `filter` function in both [`stats`](https://stat.ethz.ch/R-manual/R-devel/library/stats/html/filter.html) and [`dplyr`](https://dplyr.tidyverse.org/reference/filter.html); you can specify exactly which to use with `dplyr::filter()` or `stats::filter()`.

Note that despite being commonly used for an R DataFrame, [`df` is actually a built-in function for the F distribution](https://stat.ethz.ch/R-manual/R-devel/library/stats/html/Fdist.html). As such, it is not recommended to use `df` for DataFrames.
````{tabs}

```{code-tab} r R

?df

```
````

````{tabs}

```{code-tab} plaintext R Output
FDist                  package:stats                   R Documentation

_T_h_e _F _D_i_s_t_r_i_b_u_t_i_o_n

_D_e_s_c_r_i_p_t_i_o_n:

     Density, distribution function, quantile function and random
     generation for the F distribution with ‘df1’ and ‘df2’ degrees of
     freedom (and optional non-centrality parameter ‘ncp’).

_U_s_a_g_e:

     df(x, df1, df2, ncp, log = FALSE)
     pf(q, df1, df2, ncp, lower.tail = TRUE, log.p = FALSE)
     qf(p, df1, df2, ncp, lower.tail = TRUE, log.p = FALSE)
     rf(n, df1, df2, ncp)
     
_A_r_g_u_m_e_n_t_s:

    x, q: vector of quantiles.

       p: vector of probabilities.

       n: number of observations. If ‘length(n) > 1’, the length is
          taken to be the number required.

df1, df2: degrees of freedom.  ‘Inf’ is allowed.

     ncp: non-centrality parameter. If omitted the central F is
          assumed.

log, log.p: logical; if TRUE, probabilities p are given as log(p).

lower.tail: logical; if TRUE (default), probabilities are P[X <= x],
          otherwise, P[X > x].

_D_e_t_a_i_l_s:

     The F distribution with ‘df1 =’ n1 and ‘df2 =’ n2 degrees of
     freedom has density

     f(x) = Gamma((n1 + n2)/2) / (Gamma(n1/2) Gamma(n2/2))
         (n1/n2)^(n1/2) x^(n1/2 - 1)
         (1 + (n1/n2) x)^-(n1 + n2)/2
     
     for x > 0.

     It is the distribution of the ratio of the mean squares of n1 and
     n2 independent standard normals, and hence of the ratio of two
     independent chi-squared variates each divided by its degrees of
     freedom.  Since the ratio of a normal and the root mean-square of
     m independent normals has a Student's t_m distribution, the square
     of a t_m variate has a F distribution on 1 and m degrees of
     freedom.

     The non-central F distribution is again the ratio of mean squares
     of independent normals of unit variance, but those in the
     numerator are allowed to have non-zero means and ‘ncp’ is the sum
     of squares of the means.  See Chisquare for further details on
     non-central distributions.

_V_a_l_u_e:

     ‘df’ gives the density, ‘pf’ gives the distribution function ‘qf’
     gives the quantile function, and ‘rf’ generates random deviates.

     Invalid arguments will result in return value ‘NaN’, with a
     warning.

     The length of the result is determined by ‘n’ for ‘rf’, and is the
     maximum of the lengths of the numerical arguments for the other
     functions.

     The numerical arguments other than ‘n’ are recycled to the length
     of the result.  Only the first elements of the logical arguments
     are used.

_N_o_t_e:

     Supplying ‘ncp = 0’ uses the algorithm for the non-central
     distribution, which is not the same algorithm used if ‘ncp’ is
     omitted.  This is to give consistent behaviour in extreme cases
     with values of ‘ncp’ very near zero.

     The code for non-zero ‘ncp’ is principally intended to be used for
     moderate values of ‘ncp’: it will not be highly accurate,
     especially in the tails, for large values.

_S_o_u_r_c_e:

     For the central case of ‘df’, computed _via_ a binomial
     probability, code contributed by Catherine Loader (see ‘dbinom’);
     for the non-central case computed _via_ ‘dbeta’, code contributed
     by Peter Ruckdeschel.

     For ‘pf’, _via_ ‘pbeta’ (or for large ‘df2’, _via_ ‘pchisq’).

     For ‘qf’, _via_ ‘qchisq’ for large ‘df2’, else _via_ ‘qbeta’.

_R_e_f_e_r_e_n_c_e_s:

     Becker, R. A., Chambers, J. M. and Wilks, A. R. (1988) _The New S
     Language_.  Wadsworth & Brooks/Cole.

     Johnson, N. L., Kotz, S. and Balakrishnan, N. (1995) _Continuous
     Univariate Distributions_, volume 2, chapters 27 and 30.  Wiley,
     New York.

_S_e_e _A_l_s_o:

     Distributions for other standard distributions, including ‘dchisq’
     for chi-squared and ‘dt’ for Student's t distributions.

_E_x_a_m_p_l_e_s:

     ## Equivalence of pt(.,nu) with pf(.^2, 1,nu):
     x <- seq(0.001, 5, len = 100)
     nu <- 4
     stopifnot(all.equal(2*pt(x,nu) - 1, pf(x^2, 1,nu)),
               ## upper tails:
               all.equal(2*pt(x,     nu, lower=FALSE),
                           pf(x^2, 1,nu, lower=FALSE)))
     
     ## the density of the square of a t_m is 2*dt(x, m)/(2*x)
     # check this is the same as the density of F_{1,m}
     all.equal(df(x^2, 1, 5), dt(x, 5)/x)
     
     ## Identity:  qf(2*p - 1, 1, df) == qt(p, df)^2  for  p >= 1/2
     p <- seq(1/2, .99, length = 50); df <- 10
     rel.err <- function(x, y) ifelse(x == y, 0, abs(x-y)/mean(abs(c(x,y))))
     quantile(rel.err(qf(2*p - 1, df1 = 1, df2 = df), qt(p, df)^2), .90)  # ~= 7e-9
     

```
````
</details>

### Further Resources

PySpark Documentation:
- [`pyspark.sql.functions`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)
- [`F.col()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.col.html)
- [`F.sum()`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.sum.html)

sparklyr and tidyverse Documentation:
- [`dplyr::filter()`](https://dplyr.tidyverse.org/reference/filter.html)

Python Documentation:
- [`keyword.kwlist`](https://docs.python.org/3/library/keyword.html)

R Documentation:
- [`stats::df()`](https://stat.ethz.ch/R-manual/R-devel/library/stats/html/Fdist.html)
- [`stats::filter()`](https://stat.ethz.ch/R-manual/R-devel/library/stats/html/filter.html)