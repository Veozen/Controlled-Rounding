# Macro CtrlRound

Performs controlled rounding of a table. The result will be rounded, additive, and as controlled as possible.

## Syntax

```SAS
%CtrlRound(	
	CellIn=, 
	ConsIn=, 
	By=,
	Var=,
	Base=,
	MultiStart=,
	MaxTime=,
	Seed=,
	Radius=,
	Ctrl=,
	Adjust=,
	Solve=,
	DataOut=
);
```

## Parameters
### Input
Cell In (file) Required File
Contains the initial table.
CellId, Var= , < By= > , <Adjust>

ConsIn (file)
Contains the list of constraints.
ConstraintId, CellId, Coefficient
For a constraint a+b=c, the coefficients associated with a, b, and c are 1, 1, and -1.

By (String)
List of names of classification variables.

Var (String) Required Parameter
Name of the target variable.

Base (An integer >=1) Default=1
This is the rounding base.

MultiStart (An integer >=0) Default=1
The number of starting points used.

MaxTime (numeric >=0)
The maximum allowed execution time in minutes. (0.5 corresponds to 30 seconds)

Seed (An integer >=1) Default=1
Controls the creation of additional starting points.

Radius (An integer >=1) Default=1
Controls the extent of the neighborhood in which additional starting points are chosen.

Ctrl (Yes| 1| No |0) Default=No
Determines whether margins should be modified to satisfy the control property.

Adjust (string)
Name of the variable containing the adjustment indicator.

LPSolve (LP|MILP)
Determines the use of the LP solver.

### Output
DataOut (file) Required File
Contains the current solution.
CellId, Var= , Rnd(Var=) , < By= > , < Adjust= >

# Macro TableCons
Builds a file of linear constraints corresponding to the structure defined by classification variables.

```SAS
%TableCons(
	CellIn=, 
	by=, 
	var=, 
	ConsOut=, 
	DataOut=
);
```

## Parameters
### Input
Cell (file) Required File
Contains the initial table.
CellId, Var= , < By= > , <Adjust>

By (String) Required Parameter
List of names of classification variables.

Var (String) Required Parameter
Name of the target variable.

### Output
DataOut (file)
Contains the current solution.
CellId, Var= , Rnd(Var=) , < By= > , < Adjust= >

Cons Out (file)
Contains the list of constraints.
ConstraintId, CellId, Coefficient
For a constraint a+b=c, the coefficients associated with a, b, and c are 1, 1, and -1.
