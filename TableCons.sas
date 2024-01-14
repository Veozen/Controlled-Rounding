
/*-----------------------------------------------*/
/*  fichier de contraintes linéaire d un tableau */		
/*-----------------------------------------------*/

%macro Nobs(dataIn) ;
/*Returns the number of observations in a dataset*/
	%local dataId nobs rc;
	%let dataid=%sysfunc(open(&dataIn));
	%let nobs=%sysfunc(attrn(&dataid,nobs));
	%let rc=%sysfunc(close(&dataid));
	&nobs 
%mend Nobs;

%macro saveOptions() ;
	/*save some common options*/
	%local notes mprin symbolgen source options;
	%let notes = %sysfunc(getoption(Notes));
	%let mprint = %sysfunc(getoption(mprint));
	%let symbolgen = %sysfunc(getoption(Symbolgen));
	%let source = %sysfunc(getoption(source));

	%let options = &notes &mprint &symbolgen &source;
	&options;
%mend saveOptions;

%macro Time(from) ;
/*returns the current time  or if input provided: 
returns the elaspsed time from the input time */
	%local dataTime now time;
	%let datetime = %sysfunc( datetime() );
	%let now=%sysfunc( timepart(&datetime) );

	%if (&from ne ) %then %do;
		%let timefrom = %sysfunc(inputn(&from,time9.));
		%if %sysevalf(&now<&timefrom) %then %do;
			%let time =  %sysevalf(86400-&timefrom,ceil);
			%let time = %sysevalf(&time + %sysevalf(&now,ceil));
		%end;
		%else %do;
			%let time = %sysevalf(&now-&timefrom,ceil);
		%end;
		%let time = %sysfunc(putn(&time,time9.));
	%end;
	%else %do;
		%let time = &now;
		%let time = %sysfunc(putn(&time,time9.));
	%end;
	&time
%mend Time;

%macro Exist(chemin)  ;
	/*Check if a directory or file exists*/
	%local rc isDir fileref;
	%let isDir = 0;
	%let rc = %sysfunc (filename(fileref, &chemin));
	%if &rc = 0 %then %do;
		%if (%sysfunc(fexist(&fileref)) = 1) %then %do;
			%let isDir = 1;
		%end;
    	%let rc = %sysfunc (filename(fileref));
  	%end;
	&isDir
%mend Exist;

%macro BuildCons(workLib=, DataIn=, ClassVarList=, AnalysisVar=, ConsOut=, TableOut=) ;
/*Cette macro crée le fichier de constraintes. Elle est utilisée lorsque l utilisateur fourni un input de proc freq*/
/*Output: &ConsOut*/

	%local i k Dimensions aggregated;

	%if %sysfunc(exist(&ConsOut)) %then %do; 
		proc delete data= &ConsOut;run;	
	%end;

	%let i = 0;
	%do %until (&&Var&i = );
	  %let i = %eval(&i+1);
	  %let Var&i = %scan(&ClassVarList,&i,%str( ));
	%end;
	%let Dimensions = %eval(&i-1);

	/*test si l entrée est */
	%let aggregated=0;
	data _null_;
		set &DataIn;
		if (
		%do i = 1 %to &Dimensions;
			missing(&&Var&i) or
		%end; 
		0 ) then do;
			call symputx('aggregated',1);
		end;
	run;

	%if &aggregated eq 0 %then %do;

		/*  on calcul les totaux pour toutes les combinaisons de variables  */
		proc means data=&DataIn sum noprint;
		    class &ClassVarList;
		    var &AnalysisVar;
			output out=&workLib..FreqTable(drop=_Freq_ _Type_) sum=&AnalysisVar;
		run;

		data &workLib..FreqTable;
			set &workLib..FreqTable;
			CellId= _N_;
		run;

	%end;
	%else %do;
		data &workLib..FreqTable;
			set &dataIn;
			CellId= _N_;
			keep cellId &ClassVarList &analysisVar ;
		run;
	%end;

	data &TableOut(keep=cellId &var );
		retain cellId &var;
		set &workLib..FreqTable;
		&var=&analysisVar;
	run;

	/*création du fichier des contraintes*/
	%let NCell= %Nobs(&workLib..FreqTable);
	%let consIdStart=0;
	%do i = 1 %to &Dimensions;
		
		/*fichier qui associe une marge avec les cellules à laquelle elles s aggrègent*/
		proc sql;
			create table &workLib..CurCons as
			select a.cellId as margin , b.CellId as cellId
			from &workLib..FreqTable as a, &workLib..FreqTable as b
			where  a.cellId ne b.cellId and
					%do k = 1 %to &Dimensions;
						%if &k ne &i %then %do;
							a.&&Var&k = b.&&Var&k and
						%end;
					%end; missing(a.&&Var&i);
		quit;

		proc sort data= &workLib..CurCons; by margin;run;

		/*ajout d un identificateur de contraintes.
		Un enregistrement par contrainte est créé dans lequel on place la marge de cette contrainte.
		Un fichier contient les marges un autre contient les cellules qui s aggrègent à cette marge*/
		data &workLib..CurCons(drop=margin) &workLib..MyMargin(drop=margin);
			length Coefficient 3;
			retain consId  &ConsIdStart;
			set &workLib..CurCons end=eof;
			by margin;
			if first.margin then do;
				consId = consId+1;
				Coefficient=1;
				output &workLib..CurCons;
				cellId = margin;
				Coefficient=-1;
				output &workLib..MyMargin;
			end;
			else do;
				Coefficient=1;
				output &workLib..CurCons;
			end;
			if eof then call symputx('ConsIdStart',consId);
		run;

		/*fusion des marges et des cellules qui s y aggrègent*/
		data &workLib..CurCons;
			set &workLib..CurCons &workLib..MyMargin;
		run;

		%if %sysfunc(exist(&ConsOut)) %then %do; 
			proc append base=&ConsOut data=&workLib..CurCons;run;
		%end;
		%else %do; 
			data &ConsOut;
				set &workLib..CurCons;
			run;
		%end;
	%end;
	
	proc sort data=&ConsOut; by consId coefficient;run;
	data &ConsOut;
		retain consId cellId Coefficient;
		set &ConsOut ;
	run;

	proc delete data= &workLib..curcons &workLib..MyMargin ;run;

%mend BuildCons;

%macro GetInOutCell(workLib=,CellIn=,ConsIn=) ;
/*Sépare les cellules intérieures des marges */

	/*les marges sont faciles à identifier*/
	proc sql;
		create table &workLib..OutCell as
		select distinct CellID
		from &consIn 
		where coefficient=-1;
	quit;

	/*les cellules intérieures sont celles qui ne sont pas des marges!*/
	proc sort data= &CellIn; 			by cellId;run;
	proc sort data= &workLib..OutCell; 	by cellId;run;
	data &workLib..InCell;
		merge &CellIn(in=inA keep=cellId) &workLib..OutCell(in=inB);
		by cellId;
		if InA and not InB;
	run;

%mend GetInOutCell; 

%macro TableCons(
				CellIn=, 
				by=, 
				var=, 
				ConsOut=, 
				DataOut= ) ;
/**/
	%put;
	%put ---------------------------;
	%put Building Linear Constraints;
	%put ---------------------------;
	%put;

	%local startTime options pathOfWork workout;
	%let StartTime = %Time();
	%let options = %saveOptions();
	options nonotes nomprint;


	/*-------------Check Input and Init Variables-------------*/
	%if (&by eq  ) %then %do;
		%put ERROR: Provide by=;
		%goto exit;
	%end;
	%if (&var eq  ) %then %do;
		%put ERROR: Provide var=;
		%goto exit;
	%end;
	%if (&CellIn ne ) and ( not %sysfunc(exist(&CellIn)) ) %then %do;
		%put ERROR: File &CellIn doesn%str(%')t exist;
		%goto exit;
	%end;


	/*------------Prepare input files------------*/
	%LET pathOfWork = %sysfunc(pathname(WORK));
	%if (%Exist("&pathOfWork\CtrlRound") = 1) %then %do;
		%let workout =&pathOfWork\CtrlRound ;
		LIBNAME RoundWrk "&workout" ;
		proc datasets nolist nodetails lib=RoundWrk kill memtype=data; run; quit;
	%end;
	%else %do;
		%LET workout = %sysfunc(dcreate(CtrlRound,&pathOfWork));
		LIBNAME RoundWrk "&workout" ;
	%end;

	%put %str(  ) Problem Summary;


	/*Si un fichier de proc freq est fourni, alors il faut créer le fichier des contraintes*/
	%BuildCons(workLib=RoundWrk, dataIn=&CellIn, ClassVarList=&by , AnalysisVar=&var, ConsOut=RoundWrk.InCons,Tableout=RoundWrk.InTable);
			
	data &ConsOut;
		set RoundWrk.InCons;
	run;

	data &DataOut;
		set RoundWrk.InTable;
	run;

	data RoundWrk.NCons;
		set &ConsOut (where= (Coefficient=-1));
	run;
	
	/*Sépare les cellules intérieures des marges*/
	%GetInOutCell(workLib=RoundWrk,cellIn=RoundWrk.InTable,consIn=RoundWrk.InCons);

	/*log  message*/
	%put %str(      ) Number of Cells: %Nobs(RoundWrk.InTable) ;
	%put %str(      ) Number of Interior Cells: %Nobs(RoundWrk.Incell);
	%put %str(      ) Number of Marginal Cells: %Nobs(RoundWrk.Outcell);
	%put %str(      ) Number of Constraints: %Nobs(RoundWrk.NCons);
	%put;
	
	/*récupérer les variables de classification*/
	proc sort data=RoundWrk.FreqTable; by cellID;run;	
	proc sort data=&dataOut; by cellID;run;
	data &dataOut;
		merge &dataOut RoundWrk.FreqTable(drop=&Var);
		by cellId;
	run;
	
	proc sort data=&dataOut; by cellId;run;


	/*------------Exit------------*/

	/*Nettoyage*/
	proc datasets nolist nodetails lib=RoundWrk kill memtype=data; run; quit;
	libname RoundWrk clear;
	filename workout "&workout";
	%let rc = %sysfunc(fdelete(workout));
	%exit:

	options &options 
	%put;
	%put start at &StartTime;
	%put exit  at %Time();
	%put;
%mend TableCons;

/*                Exemple               */
/*create some input files*/
data Population;
	do i = 1 to 10000;
		var1=rand('table',0.14,0.14,0.14,0.14,0.14,0.14,0.16);
		var2=rand('table',0.14,0.14,0.14,0.14,0.14,0.14,0.16);
		var3=rand('table',0.1,0.1,0.1,0.1,0.1,0.1,0.4);
		var4=rand('table',0.1,0.1,0.1,0.1,0.1,0.1,0.4);
		var5=rand('table',0.1,0.1,0.1,0.1,0.1,0.1,0.4);
		var6=rand('table',0.1,0.1,0.1,0.1,0.1,0.1,0.4);
		var7=rand('table',0.1,0.1,0.1,0.1,0.1,0.1,0.4);
		var8=rand('table',0.1,0.1,0.1,0.1,0.1,0.1,0.4);
		output;
	end;
run;


proc freq data=Population noprint;
	table var1*var2*var3/ out=myfreq(drop=percent) sparse;  *v3*v4/ out=myfreq sparse;
run;

%TableCons(
			CellIn= myfreq, 
			by= var1 var2 var3,  
			var=count,
			ConsOut= myCons, 
			DataOut= mynewTable );

			
			
