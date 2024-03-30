What is MCABOR ?
===================

Name
------------------------------------------
McVerry's Cointegration Algorithm Based On Relativity

Description
------------------------------------------
MCABOR is a simple cointegration calculator based on the Java language.   
[GPL License](LICENSE).


Reference
------------------------------------------
Refer to the github repository for all updates and information

Author
------------------
Joe McVerry (usacoder@gmail.com)

I wrote this to help me with building a Java based pairs trading program.  Let me know how you use it.

MCABOR How It Works
---------------------

- This is not based on any formal mathematical cointegration models such as Engle–Granger, Johansen, etc.

- It takes two Java ArrayList objects containing Doubles, there must be more than one element in the lists, and equal number of elemtns, and there must be
unique minimum and maximum values.   

- iterates through both lists to find the minimum and maximum values for each.

- iterates again to compute each elements relative value to the minimum and maximum, the computed value will be 
between 0 and 1.

   * during this iteration it sums the absolute value of the difference of all of the two relative values based on their position.

- it returns the sum divided by the number of elements in the ArrayLists.


Supported Language
------------

Java


Build MCABOR
---------------

Please use [build guide](pom.xml) to build MCABOR from source.


Contribution
------------

Contributions are welcome. 

