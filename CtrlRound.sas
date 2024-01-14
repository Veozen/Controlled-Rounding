
/*-------------------------*/
/* Controlled Rounding     */		
/*-------------------------*/

libname mylib 'F:\CtrlRound';
options mstored sasmstore=mylib;

%macro Nobs(dataIn) /store ;
/*Returns the number of observations in a dataset*/
	%local dataid nobs rc;
	%let dataid=%sysfunc(open(&dataIn));
	%let nobs=%sysfunc(attrn(&dataid,nobs));
	%let rc=%sysfunc(close(&dataid));
	&nobs 
%mend Nobs;

%macro saveOptions() /store;
	/*save some common options*/
	%local notes mprint symbolgen source options;
	%let notes = %sysfunc(getoption(Notes));
	%let mprint = %sysfunc(getoption(mprint));
	%let symbolgen = %sysfunc(getoption(Symbolgen));
	%let source = %sysfunc(getoption(source));

	%let options = &notes &mprint &symbolgen &source;
	&options;
%mend saveOptions;

%macro Time(from) /store;
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

%macro ListLen(list,sep=%str( )) /store;
/*returns the length of a list*/
	%local word count;
	%let word=%scan(&List, 1, &sep);
	%let count=0;
	%do %while(&word ne );
		%let count=%eval(&count+1);
		%let word=%scan(&List, %eval(&count+1), &sep);
	%end;
	&count
%mend ListLen;

%macro VarNames(Data) /store;
	/*Generates the complete list of column names in a SAS dataset */
	%local VarList CurrentVar DSID NumVars;
	%let DSID = %sysfunc(open(&Data));
	%let NumVars = %sysfunc(attrn(&DSID, nvars));

	/* loop through all variables and get their names */
	%let CurrentVar = 1;
	%do %while(&CurrentVar <= &NumVars);
		%let VarList = &VarList %sysfunc(varname(&DSID, &CurrentVar));
		/* append current variable's name to output list */
		%let CurrentVar = %eval(&CurrentVar + 1);
	%end;

	%let DSID = %sysfunc(close(&DSID));
	&VarList 
%mend VarNames;

%macro Exist(chemin) /store ;
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

%macro TimeCheck() /store;
	%sysevalf(%sysevalf(%sysfunc(inputn(%time(&startTime),time9.))/60) < &MaxTime)
%mend TimeCheck;

%macro CleanCons(workLib=,TableIn=,ConsIn=,OutCons=,OutTable=) /store;
/*Clean up constraints produced by artificial aggregates*/
	%local dsId vtype rc consIdnum renamecons;
	%let dsId = %sysfunc(open(&TableIn));
	%let vtype = %sysfunc(varnum(&dsId,type));
	%let rc= %sysfunc(close(&dsId));


	%let dsid = %sysfunc(open(&ConsIn));
	%let consIdnum = %sysfunc(varnum(&dsId,consId));
	%if &consIdnum=0 %then %let renameCons = ;
	%else %let renameCons = (rename=(consId=constraintId));/*j ai utilisé consId plutot que constraintId */
	%let rc = %sysfunc(close(&dsId));
	data &consIn;
		set &consIn &renameCons;
	run;

	/*si par hasard la variable type ne se trouve pas sur le tableau d entrée, alors on ne fait rien*/
	%if (&vtype eq 0) %then %do;
	
		data &OutTable;
			set &TableIn;
		run;
		
		proc sql;
			create table &OutCons as
			select  cellId, coefficient, constraintId as consId
			from &consIn  ;
		quit;
	%end;
	%else %do;
		/*Remove artificial aggregates from table*/
		data &OutTable;
			set &TableIn;
			where Type ne 'A';
		run;

		proc sql;
			create table &workLib..aggregates as
			select cellid
			from &tableIn
			where type='A';
		quit;
		%if (&sqlobs ne 0) %then %do;
			/*on obtient les constraintes associées à ces agrégats puis on les élimine du fichier des contraintes*/
			proc sql;
				create table &workLib..consAggregates as
				select distinct constraintId
				from &consIn &renameCons as c , &workLib..aggregates as a
				where c.cellid = a.cellId;
			quit;


			/*on garde les enregistrements qui on une contrainte que l on ne retrouve pas dans le fichier précédent*/
			proc sort data=&consIn; by constraintId;run;
			proc sort data=&workLib..consAggregates; by constraintId;run;
			data &OutCons(rename=(constraintId=consId));
				merge &consIn(in=inA) &workLib..consAggregates(in=inB);
				by constraintId;
				if inA and not inB;
			run;

			proc delete data= &workLib..consAggregates ;run;
		%end;
		%else %do;
			proc sql;
				create table &OutCons as
				select distinct cellid, coefficient, constraintId as consId
				from &consIn  ;
			quit;
		%end;

		proc delete data= &workLib..aggregates ;run;
	%end;

%mend CleanCons;

%macro BuildCons(workLib=, DataIn=, ClassVarList=, AnalysisVar=, ConsOut=, TableOut=) /store;
/*Creates constraints file */
/*Output: &ConsOut*/

	%local i k Dimensions ;

	%if %sysfunc(exist(&ConsOut)) %then %do; 
		proc delete data= &ConsOut;run;	
	%end;

	%let dimensions = %listlen(&ClassVarList);
	%do i = 1 %to &Dimensions; %local  Var&i; %end; 
	%do i = 1 %to &Dimensions; 
	  	%let Var&i = %scan(&ClassVarList,&i,%str( ));
	%end;

	/*Filter totals*/

	%let ReMargins=0;
	data &worklib..Filtered &worklib..FilteredMargins;
		set &DataIn;
		if (
		%do i = 1 %to &Dimensions;
			missing(&&Var&i) or
		%end; 
		0 ) then do;
			call symputx('ReMargins',1);
			output &worklib..FilteredMargins;
		end;
		else do;
			output &worklib..Filtered;
		end;
	run;


	/*Calculate totals for all variables combinations */
	proc means data=&worklib..Filtered sum noprint;
	    class &ClassVarList;
	    var &AnalysisVar;
		output out=&workLib..FreqTable(drop=_Freq_ _Type_) sum=&AnalysisVar;
	run;

	data &workLib..FreqTable;
		set &workLib..FreqTable;
		CellId= _N_;
	run;

	data &TableOut(keep=cellId &var );
		retain cellId &var;
		set &workLib..FreqTable;
		&var=&analysisVar;
	run;

	/*Constraints file creation*/
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

		/*Merge margins and the cells that add up to it*/
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

	proc delete data= &workLib..curcons &workLib..MyMargin &worklib..Filtered;run;

%mend BuildCons;

%macro GetInOutCell(workLib=,CellIn=,ConsIn=) /store;
/*Split interior cells from margins */

	/*margins are easy to identify*/
	proc sql;
		create table &workLib..OutCell as
		select distinct CellID
		from &consIn 
		where coefficient=-1;
	quit;

	/*interior cells are those that are not margins*/
	proc sort data= &CellIn; 			by cellId;run;
	proc sort data= &workLib..OutCell; 	by cellId;run;
	data &workLib..InCell;
		merge &CellIn(in=inA keep=cellId) &workLib..OutCell(in=inB);
		by cellId;
		if InA and not InB;
	run;

%mend GetInOutCell; 

%macro MakeAdjust(workLib=) /store;
/*If user wants to round only a part of the cells, then a file is created for that purpose.
	Only interior cells are considered*/
	%if (%upcase(&AdjustAll) = FALSE) %then %do; 
		data &workLib..InCellAdjust;
			set &workLib..InTable;
		run;

		proc sort data=&workLib..InCellAdjust; by cellId;run;
		proc sort data=&workLib..OutCell; by cellId;run;
		data &workLib..InCellAdjust;
			merge &workLib..InCellAdjust(in=inA) &workLib..outCell(in=inB);
			if inA and not inB;
		run;
	%end;
%mend MakeAdjust;

%macro MakeIncellMargins(workLib=,ConsIn=) /store;
/*create the file that associate interior cells with their margins*/
/*Output:InCellMargin*/
	/*%let Mstart = %time();*/
		/*On démare avec les cellules intérieure comme cellules courantes*/
		data &workLib..ToMarginId0;
			set &workLib..inCell;
			incell=CellId;
		run;
		
		data &workLib..OutConstraints (drop = coefficient);
			set &consIn(where= (coefficient=-1));
		run;

		data &workLib..InConstraints (drop = coefficient);
			set &consIn(where= (coefficient=1));
		run;
		/*proc sort data=&workLib..InConstraints; by cellId;run;*/

		%local i continue;
		%let i=0;
		%let continue=1;
		%do %while(&continue=1);
			/*Obtain current cell's constraints identifiers*/
			proc sql;
				 create table &workLib..ToConsId&i as
				 select a.incell, b.consId
				 from &workLib..ToMarginId&i as a, &workLib..InConstraints as b
				 where a.cellId=b.cellId ;
				 /*order by a.incell;*/
			quit;
			%let continue=0;

			/*Reduce file containing InCons cells*/
			/*data &workLib..InConstraints;
				merge &workLib..InConstraints(in=inA) &workLib..ToConsId&i(in=inB keep=incell rename=(incell=cellId));
				by cellId;
				if inA and not inB;
			run;*/

			%if &sqlobs>0 %then %do;
				%let i = %eval(&i+1);
				%let continue=1;

				/*avec les identificateurs de contrainte des cellules courantes, on va chercher les marges auxquelle cette cellule s aggrège*/
				/*Les marges deviendront les nouvelles cellules courantes de la prochaine itération*/
				proc sql;
					create table &workLib..ToMarginId&i as
					select a.incell, b.cellId
					from &workLib..ToConsId%eval(&i-1) as a, &workLib..OutConstraints as b
					where a.consId=b.consId ;
				quit;

				/*save those pairs*/
				proc sort data=&workLib..ToMarginId&i noduprecs; by incell cellId;run;
				%if &i=1 %then %do; 
					data &workLib..IncellMargin;
						set &workLib..ToMarginId&i;
					run;
				%end;
				%else %do;
					proc append base=&workLib..IncellMargin data=&workLib..ToMarginId&i;run;
				%end;
			%end;
		%end;
		proc sort data=&workLib..IncellMargin noduprecs; by incell cellId;run;
		proc delete data= %do j = 0 %to &i; &workLib..ToMarginId&j  &workLib..ToConsId&j %end;  &workLib..OutConstraints &workLib..InConstraints;run;
		/*%put %nobs(&workLib..IncellMargin);
		%put MakeIncell %time(&MStart);*/
%mend MakeIncellMargins;

%macro AddCtrl(DataIn=, ConsIn=, var=, base=, DataOut=) /store;
/*Adjust output file to make sure it is controlled*/

	%local  N Nadd options;

	/*apply control on margins*/
	data &dataOut(drop=lowres dist);
		set &dataIn;
		lowres = mod(&var,&Base);
		dist = abs(rnd&var-&var);
		if lowres=0 then rnd&var=&var;
		else if dist< &Base then rnd&var=rnd&var;
			else if rnd&var<&var then rnd&var=&var-mod(&var,&Base);
			else rnd&var=&var-mod(&var,&Base)+&base;
	run;

	/*Computing Additivity Rate*/
	proc sort data=&consIn; by cellId;run;
	proc sort data=&dataOut; by cellId;run;
	data __addRate__(keep=consId cellId prod);
		merge &consIn(in=inA) &dataOut(keep= cellId &var rnd&var) ;
		by cellId;
		if inA;
		prod= coefficient*rnd&var;
	run;

	proc sort data=__addRate__; by consId;run;
	proc means data= __addRate__ noprint; by consId;var prod ; output out= __added__  sum=sum;run;
	%let N=%Nobs(__added__);

	data __ReducedAdded__;
		set __added__;
		if sum=0;
	run;

	%let Nadd=%Nobs(__ReducedAdded__);
	%let addRate = %sysevalf(&Nadd/&N);

	proc delete data= __addRate__ __added__ __ReducedAdded__;run;

%mend AddCtrl;

%macro BuildTable(worklib=, DataIn=, invar=, DataOut=) /store;
	%if (&invar eq ) %then %do;
		%let invar=rnd&var;
	%end;
	/*Prepare file containing margin information*/

	%if (&dataOut ne &dataIn) %then %do;
		%if %sysfunc(exist(&dataOut)) %then %do;
			proc sort data=&DataIn; by cellId;run;
			proc sort data=&dataOut; by cellId;run;
			data &dataOut;
				update &dataOut &DataIn;
				by cellId;
			run;
		%end;
		%else %do;
			data &dataOut;
				set &workLib..outcell &workLib..incell;
			run;
			proc sort data=&dataOut; by cellId;run;
		%end;
	%end;

	proc sort data=&workLib..Outcell; by cellId;run;
	data &workLib..outCell(keep=MarginId _MarginTotal );
		set &workLib..outcell ;
		MarginId=cellId;
		_MarginTotal=0;
	run;

	data &workLib..inCellMarginMap;
		set &workLib..inCellMargin(rename=(cellId=marginId incell=cellId ));
	run;

	proc sort data=&DataIn; by cellId;run;
	proc sort data=&workLib..incell; by cellId;run;
	data &workLib..InCell;
		merge &workLib..incell(in=inA ) &DataIn(in=inb keep=cellId &var &invar);
		by cellId;
		if inA;
	run;
	%let Ncell=%Nobs(&workLib..Incell);

	%let hashSizeIn = %sysfunc(ceil(%sysevalf(%sysfunc(log(%sysevalf(%Nobs(&worklib..inCellMarginMap)/1000))) / %sysfunc(log(2)))));
	/*%put HashSize : &hashSizeIn;*/
	%let hashSizeOut = %sysfunc(ceil(%sysevalf(%sysfunc(log(%sysevalf(%Nobs(&worklib..outCell)/1000))) / %sysfunc(log(2)))));
	/*%put HashSize : &hashSizeOut;*/

	/**/
	data &workLib..InCell(keep=cellId &var &invar);
		if _N_=1 then do;
		declare hash inCellMargin(dataset:"&worklib..inCellMarginMap" , multidata: 'y'); /* , hashexp : &hashSizeIn);*/
			inCellMargin.defineKey('CellId');
			inCellMargin.defineData('MarginId');
			inCellMargin.defineDone();
		 	call missing (CellId, MarginId);
		declare hash outCell(dataset:"&worklib..outCell"); /* , hashexp : &hashSizeOut);*/
			outCell.defineKey('MarginId');
			outCell.defineData('MarginId','_MarginTotal');
			outCell.defineDone();
			call missing (MarginId, _MarginTotal);
		end;

		/*for each interior cell*/
		set &workLib..InCell(keep=cellId &var &invar) end=eof;	

		/*find associated margins*/
		rc= inCellMargin.find();
		if (rc=0) then do;
			
			/*Obtain associated margins and accumulate the distance for those margins, 
			one distance for down-rounding, another for up-rounding*/
			outcell.find();

			/*update margins*/
			_MarginTotal= _MarginTotal + &invar;
			outcell.replace();	
			incellMargin.has_next(result: r);
			do while ( r ne 0 );
				incellMargin.find_next();
				incellMargin.has_next(result: r);
				outcell.find();	
				_MarginTotal= _MarginTotal + &invar;
				outcell.replace();	
			end;
		end;
		
		if eof then do;
			outcell.output(dataset:"&worklib..outCell(rename=(MarginId=cellId _MarginTotal=rnd&var))");
		end;
	run;
	

	/*Retreive results*/
	proc sort data=&workLib..outCell; by cellId;run;
	proc sort data=&dataOut; by cellId;run;
	data &dataOut;
		update &dataOut &worklib..outCell;
		by cellId;
		rnd&var=roundz(rnd&var,1e-12);
	run;

	data &worklib..outCell (keep=cellId &var rnd&var);
		merge &worklib..outCell(in=inA) &dataOut ;
		by cellId;
		if inA;
	run;

%mend BuildTable;

%macro GetDistance(workLib=, dataIn= ,dist=) /store;
/*Calculate the distance of the whole table*/

	data _null_;
		set &dataIn end=eof;
		retain _sum 0;
		_dist= abs(rnd&var-&var);
		_res=mod(&var,&base);
		if _res > 0 then do;
			if _dist <= &base then do;
				_dist=0;
			end;
			else if rnd&var>&var then do;
				_dist=_dist-(&base-_res);
			end;
			else do;
				_dist=_dist-_res;
			end;
		end;
		
		_dist=roundz(_dist,1e-4);
		_sum=_sum+_dist;
		if eof then do;
			_sum=roundz(_sum,1e-2)/&base;
			call symputx("&dist",_sum);
		end;
	run;

%mend GetDistance;

%macro GetInfoLoss(workLib=, dataIn= ,dist=) /store;
/*Calculate information loss for the whole table*/

	data _null_;
		set &dataIn end=eof;
		retain _sum 0;
		_dist= abs(rnd&var-&var);
		
		_dist=roundz(_dist,1e-4);
		_sum=_sum+_dist;
		if eof then do;
			_sum=roundz(_sum,1e-2)/&base;
			call symputx("&dist",_sum);
		end;
	run;

%mend GetInfoLoss;

%macro UpdateOutput(workLib=, dataIn= , dataOut=) /store;
	proc sort data=&workLib..incell; by cellId;run;
	proc sort data=&dataIn; by cellId;run;
	data &dataOut(keep=cellId &var rnd&var &adjust);
		merge &dataIn(in=inA) &workLib..incell;
		by cellId;
		if inA;
	run;
	proc sort data=&workLib..outCell; by cellId;run;
	data &dataOut;
		merge &dataOut(in=inA) &worklib..outCell(keep=cellId &var rnd&var);
		by cellId;
		if inA;
	run;
	data &dataOut;
		retain cellId &var rnd&var;
		set &dataOut;
	run;
%mend UpdateOutPut;
%macro UpdateInput(workLib=, dataOut=) /store;

	proc sort data=&dataOut; by cellId;run;
	proc sort data=&workLib..incell; by cellId;run;
	data &workLib..InCell;
		merge &workLib..incell(in=inA keep=cellId) &dataOut(in=inb keep=cellId rnd&var &var);
		by cellId;
		if inA;
	run;

	%if %upcase(&AdjustAll)=FALSE %then %do;
		proc sort data=&workLib..InCellAdjust; by cellId;run;
		proc sort data=&workLib..incell; by cellId;run;		
		data &workLib..InCell(drop= &adjust);
			merge &workLib..InCell(in=inA ) &workLib..InCellAdjust(keep = cellId &adjust);
			by cellId;
			if inA;
			if &adjust=1;
		run;
	%end;

	proc sort data=&workLib..Outcell; by cellId;run;
	data &workLib..outCell(keep=MarginId _MarginTotal _OriginalTotal);
		merge &workLib..outcell(in=inA) &dataOut(in=inb keep=cellId rnd&var &var );
		by cellId;
		if inA;
		MarginId=cellId;
		_OriginalTotal=&var;
		_MarginTotal=rnd&var;
	run;

	data &workLib..inCellMarginMap;
		set &workLib..inCellMargin(rename=(cellId=marginId incell=cellId ));
	run;
%mend UpdateInput;

%macro SQRound(workLib=, DataIn=, Base= ) /store;
/*Performs sequential rounding */

	/*%let sqStart = %time();*/
	/*If rounding should be done on a subset*/
	%if %upcase(&AdjustAll)=FALSE %then %do;
		proc sort data=&workLib..InCellAdjust; by cellId;run;
		proc sort data=&workLib..incell; by cellId;run;		
		data &workLib..InCell(drop= &adjust );
			merge &workLib..InCell(in=inA ) &workLib..InCellAdjust(keep = cellId &adjust);
			by cellId;
			if inA;
			if &adjust=1;
		run;
	%end;
	

	/*Prepare file containing margin information*/
	proc sort data=&workLib..Outcell; by cellId;run;
	data &workLib..outCell(keep=MarginId _MarginTotal _OriginalTotal);
		/*length marginId $&lengthcell;*/
		merge &workLib..outcell(in=inA) &DataIn(in=inb keep=cellId &var );
		by cellId;
		if inA;
		MarginId=cellId;
		_OriginalTotal=&var;
		_MarginTotal=&var;
	run;


	data &workLib..inCellMarginMap;
		set &workLib..inCellMargin(rename=(cellId=marginId incell=cellId ));
	run;


	/*Order variables*/
	/*Calculate residuals */
	proc sort data=&DataIn; by cellId;run;
	proc sort data=&workLib..incell; by cellId;run;
	data &workLib..InCell(drop= res upres);
		merge &workLib..incell(in=inA) &DataIn(in=inb keep=cellId &var);
		by cellId;
		if inA;
		lowres	= mod(&var,&base);
		upres=&base-lowres;
		res= min(lowres,upres);
		order=res;
	run;
	proc sort data=&workLib..InCell ; by order;run;
	%let Ncell=%Nobs(&workLib..Incell);

	%let hashSizeIn = %sysfunc(ceil(%sysevalf(%sysfunc(log(%sysevalf(%Nobs(&worklib..inCellMarginMap)/1000))) / %sysfunc(log(2)))));
		/*%put HashSize : &hashSizeIn;*/
	%let hashSizeOut = %sysfunc(ceil(%sysevalf(%sysfunc(log(%sysevalf(%Nobs(&worklib..outCell)/1000))) / %sysfunc(log(2)))));
		/*%put HashSize : &hashSizeOut;*/

	/*Sequential Rounding*/
	data &workLib..InCell(keep=cellId &var rnd&var);
		if _N_=1 then do;
		declare hash inCellMargin(dataset:"&worklib..inCellMarginMap" , multidata: 'y'); /* , hashexp : &hashSizeIn);*/
			inCellMargin.defineKey('CellId');
			inCellMargin.defineData('MarginId');
			inCellMargin.defineDone();
		 	call missing (CellId, MarginId);
		declare hash outCell(dataset:"&worklib..outCell"); /* , hashexp : &hashSizeOut);*/
			outCell.defineKey('MarginId');
			outCell.defineData('MarginId','_MarginTotal','_OriginalTotal');
			outCell.defineDone();
			call missing (MarginId, _MarginTotal, _OriginalTotal);
		end;

		/*for each interior cell*/
		set &workLib..InCell(keep=cellId &var lowres ) end=eof;	

		/*find assiociated margins*/
		rc= inCellMargin.find();
		if (rc=0) then do;
			
			/*on obtient les marges associés et on accumule la distance pour ces marges, 
			une distance qui correspond à un arrondissement vers le bas, l autre distance pour un arrondissement vers le haut*/
			outcell.find();
			distLow =abs(_MarginTotal-_OriginalTotal-lowres);
			distUp =abs(_MarginTotal-_OriginalTotal+ (&base-lowres));

			/*while there are other margins*/
			incellMargin.has_next(result: r);
			do while ( r ne 0 );
				incellMargin.find_next();
				incellMargin.has_next(result: r);
				outcell.find();
				distLow = 	sum(distLow, 	abs(_MarginTotal-_OriginalTotal-lowres));
				distUp = 	sum(distUp, 	abs(_MarginTotal-_OriginalTotal+ (&base-lowres)));			
			end;

			/*If distance for down-rounding is lower then:*/	
			if (distLow<distUp) then do;	
			
				/*update margins*/
				_MarginTotal= _MarginTotal-lowres;
				outcell.replace();	
				incellMargin.has_prev(result: r);
				do while ( r ne 0 );
					incellMargin.find_prev();
					incellMargin.has_prev(result: r);
					outcell.find();	
					_MarginTotal= _MarginTotal-lowres;
					outcell.replace();	
				end;
				/*round interior cell*/
				rnd&var=&var -lowres;

			end;
			else do;/*alors distLow>=distUp, donc on arrondie vers le haut*/

				/*update margins*/
				_MarginTotal= _MarginTotal+ (&base-lowres);
				outcell.replace();
				incellMargin.has_prev(result: r);
				do while ( r ne 0 );
					incellMargin.find_prev();
					incellMargin.has_prev(result: r);
					outcell.find();	
					_MarginTotal= _MarginTotal+ (&base-lowres);
					outcell.replace();	
				end;
				/*round interior cell*/
				rnd&var=&var + (&base-lowres);
			end;
	
		end;

		
		if eof then do;
			/*itemSize=incellMargin.Item_size;
			numitems=incellMargin.num_items;
			inCellSize=itemSize*numItems;

			itemSize=outcell.Item_size;
			numitems=outcell.num_items;
			outCellSize=itemSize*numItems;
			TotalSize=IncellSize+OutCellSize;
			put TotalSize=;*/

			outcell.output(dataset:"&worklib..outCell(rename=(MarginId=cellId _MarginTotal=rnd&var _OriginalTotal=&var))");
		end;
	run;


	/*%put %time(&SqStart);*/
%mend SQRound;

%macro ITARound(workLib=, Base=, distance= , infoLoss=) /store;
/*Performs iterative sequential rounding */

	%local Ncell;

	/*Iterative Adjustement*/
	%let Ncell=%Nobs(&workLib..Incell);
	data &workLib..InCell(drop= res upres lowres);
		set &workLib..InCell;
		lowres	= mod(&var,&base);
		upres=&base-lowres;
		res= roundz(min(lowres,upres),1e-4);
		order=res;
		/*if res > 0;*/
	run;
	%let Ncell=%Nobs(&workLib..Incell);

	proc sort data=&workLib..InCell ; by order;run;
	data &workLib..InCell(keep=cellId &var rnd&var);
		if _N_=1 then do;
			declare hash inCellMargin(dataset:"&worklib..inCellMarginMap" , multidata: 'y');
				inCellMargin.defineKey('CellId');
				inCellMargin.defineData('MarginId');
				inCellMargin.defineDone();
			 	call missing (CellId, MarginId);
			declare hash outCell(dataset:"&worklib..outCell");
				outCell.defineKey('MarginId');
				outCell.defineData('MarginId','_MarginTotal','_OriginalTotal');
				outCell.defineDone();
				call missing (MarginId, _MarginTotal, _OriginalTotal);
		end;

		totalDistance=&Distance;
		TotalInfoLoss=&InfoLoss;
		_count=0;
		_improvement=1;
		_ctrl=0;
		_alarm=0;
		
		/*do while there is improvement*/
		do until (_improvement=0  or _ctrl=1 or _alarm>=&NCell);
			_improvement=0;
			/*for all cells*/
			do i = 1 to &NCell;
				/*fetch an interior cell*/
				modify &workLib..InCell(keep=cellId &var rnd&var) point=i;
				retain _prevCell;

				/*on identifie les marges qui y sont associées, il y en a au moins une*/
				rc= inCellMargin.find();
				if (rc=0 and rnd&var ne &var) then do;
					/*Fetch this margins value*/
					outcell.find();
					incellMargin.has_next(result: r);
					if (rnd&var > &var) then do; 
						_round= -&base;
					end;
					else do;
						_round= &base ; 
					end;

					/*calculate current distance*/
					_OldInfoLoss=abs(rnd&var -&var);
					_NewInfoLoss=abs(rnd&var + _round -&var);

					_res= mod(_OriginalTotal,&base);
					_dist = abs(_MarginTotal-_OriginalTotal );
					_infoLossOld = _dist ;
					if _res>0 then do;
						if _MarginTotal>_OriginalTotal then do; 
							_res= &base-_res; 
						end;
						if _dist<&base then do;
							_dist=0;
						end;
						else do;
							_dist=_dist-_res;
						end;
					end;

					/*as well as the new distance resulting from the modification*/
					_res= mod(_OriginalTotal,&base);
					_newdist =  abs(_MarginTotal-_OriginalTotal  + _round);
					_infoLossNew = _newDist;
					if _res>0 then do;
						if (_MarginTotal+_round)>_OriginalTotal then do; 
							_res= &base-_res; 
						end;
						if _newdist<&base then do;
							_newdist=0;
						end;
						else do;
							_newdist=_newdist-_res;
						end;
					end;

					OldDistMax=_dist;
					OldDistSum=_dist;
					NewDistMax=_newdist;
					NewDistSum=_newdist;
					_OldInfoLoss = 	sum(_OldinfoLoss, 	_infoLossOld );
					_NewInfoLoss = 	sum(_NewinfoLoss, 	_infoLossNew );

					/*Do the same thing for all the margins associated to that interior cell*/
					do while ( r ne 0 );
						incellMargin.find_next();
						incellMargin.has_next(result: r);
						outcell.find();
						
						_res= mod(_OriginalTotal,&base);
						_dist = abs(_MarginTotal-_OriginalTotal );
						_infoLossOld=_dist;
						if _res>0 then do;
							if _MarginTotal>_OriginalTotal then do; 
								_res= &base-_res; 
							end;
							if _dist<&base then do;
								_dist=0;
							end;
							else do;
								_dist=_dist-_res;
							end;
						end;

						_res= mod(_OriginalTotal,&base);
						_newdist =  abs(_MarginTotal-_OriginalTotal + _round);
						_infoLossNew =_newDist;
						if _res>0 then do;
							if (_MarginTotal+_round)>_OriginalTotal then do; 
								_res= &base-_res; 
							end;
							if _newdist<&base then do;
								_newdist=0;
							end;
							else do;
								_newdist=_newdist-_res;
							end;
						end;
						
						/*Accumulate all distances*/
						OldDistMax =	max(OldDistMax, 	_dist);
						OldDistSum =	sum(OldDistSum, 	_dist);
						NewDistMax = 	max(NewDistMax, 	_newdist );
						NewDistSum = 	sum(NewDistSum, 	_newdist );

						_OldInfoLoss = 	sum(_OldinfoLoss, 	_infoLossOld );
						_NewInfoLoss = 	sum(_NewinfoLoss, 	_infoLossNew );
					end;

					/*If new distance is better that the old one, then it's worth performing the change to that interior cell*/
					if (%if (&DistMax eq 1) %then %do; NewDistSum<=OldDistSum  /*and NewDistMax<=OldDistMax*/ %end; %else %do; _NewInfoLoss<=_OldInfoLoss %end;) then do; 
						_improvement=1;	
						_count=_count+1;

						totalDistance=TotalDistance - roundz((OldDistSum-NewDistSum)/&base,1e-3);
						totalinfoLoss=totalinfoLoss - roundz((_OldInfoLoss-_NewInfoLoss)/&base,1e-3);
						/*
						put @8 "Iteration: " _count @32 "Distance: " TotalDistance ;
						*/

						/*If stopping criterion is met, exit loop*/
						if ( abs(totalDistance)< 1e-4) then do; 
							_ctrl=1;
							i=&NCell; 
						end;
						/*to avoid cycles*/
						if (%if (&DistMax eq 1) %then %do; NewDistSum eq OldDistSum %end; %else %do; abs(_NewInfoLoss-_OldInfoLoss)/&base <=1e-4 %end;) then do; 
							_alarm=_alarm+1; 
						end;
						else do;
							_alarm=0;
						end;
						if _prevCell=CellId then do; 
							_alarm=&NCell;
						end;
						_prevCell=CellId;

						/*update margins to reflect that change*/
						_MarginTotal= _MarginTotal + _round;
						outcell.replace();	
						incellMargin.has_prev(result: r);
						do while ( r ne 0 );
							incellMargin.find_prev();
							incellMargin.has_prev(result: r);
							outcell.find();	
							_MarginTotal= _MarginTotal + _round;
							outcell.replace();	
						end;
						/*update this interior cell*/
						rnd&var=rnd&var + _round;
						replace;
					end;
				
				end;
			end;
		end;
	
		outcell.output(dataset:"&worklib..outCell(rename=(MarginId=cellId _MarginTotal=rnd&var _OriginalTotal=&var))");
		call symputx("Distance",TotalDistance);
		call symputx("infoLoss",TotalinfoLoss);
		call symputx("ITAcount",_count);
		stop;
	run;


%mend ITARound;

%macro MultiRound(workLib=, Base=, DataOut= ) /store;
/*Performs iterative sequential rounding */

	%local MultiCount Distance infoLoss ITAcount NCell;
	%let MultiCount=2;
	%let Distance= &totalDistance;
	%let infoLoss = &totalinfoLoss;

	/*Retreive results from sequential rounding*/
	proc sort data=&dataOut; by cellId;run;
	proc sort data=&workLib..incell; by cellId;run;
	data &workLib..InCell;
		merge &workLib..incell(in=inA keep=cellId) &dataOut(in=inb keep=cellId rnd&var &var);
		by cellId;
		if inA;
	run;

	%if %upcase(&AdjustAll)=FALSE %then %do;
		proc sort data=&workLib..InCellAdjust; by cellId;run;
		proc sort data=&workLib..incell; by cellId;run;		
		data &workLib..InCell(drop= &adjust);
			merge &workLib..InCell(in=inA ) &workLib..InCellAdjust(keep = cellId &adjust);
			by cellId;
			if inA;
			if &adjust=1;
		run;
	%end;

	data &workLib..inCellMarginMap;
		set &workLib..inCellMargin(rename=(cellId=marginId incell=cellId ));
	run;

	proc sort data=&workLib..incell; by cellId;run;
	data &workLib..inCell(keep=cellId rnd&var &var order);
		merge &workLib..incell(in=inA keep=cellId) &dataOut(in=inb keep=cellId rnd&var &var );
		by cellId;
		if inA;
		lowres	= mod(&var,&base);
		upres=&base-lowres;
		res= roundz(min(lowres,upres),1e-4);
		order=res;
		/*if res > 0;*/
	run;
	%let Ncell=%Nobs(&workLib..Incell);
	proc sort data=&workLib..InCell ; by order;run;
	
	%do %while( ((&MultiCount le &MultiStart) or (&MultiStart eq ) ) and &distance gt 0 and ( (&MaxTime eq ) or (%TimeCheck())));

		/*pick one cell at random and modify it*/
		data &workLib..InCell(keep=cellId &var rnd&var);
			call streaminit(&seed);
			do _NCount = 1 to &radius;
				p = ceil(rand('uniform')* &Ncell);
				
				modify &workLib..InCell(keep=cellId &var rnd&var) point=p;
				if (rnd&var ne &var) then do;
					/*mettre à jour la cellule intérieure*/
					if (rnd&var > &var) then do; 
						_round= -&base;
					end;
					else do;
						_round= &base ; 
					end;
					rnd&var=rnd&var + _round;
				end;
				replace;
			end;
			stop;
		run;
		/*
		%put &infoLoss;
		%put &Distance;*/
		%BuildTable(workLib=&worklib, dataIn=&workLib..InCell, dataOut=RoundWrk.temp&dataOut);
		%GetDistance(workLib=&worklib, dataIn=RoundWrk.temp&dataOut ,dist=Distance);
		%GetInfoLoss(workLib=&worklib, dataIn=RoundWrk.temp&dataOut ,dist=InfoLoss);
		/*
		%put : &infoLoss;
		%put : &Distance;*/
		%let seed= %eval(&seed+1);

		/*Iterative Adjustement*/
		%UpdateInput(workLib=RoundWrk, dataOut=RoundWrk.temp&dataOut);
		%ITARound(workLib=RoundWrk, Base=&Base, distance=&TotalDistance, infoLoss=&totalInfoLoss);
		%UpdateOutput(workLib=RoundWrk, dataIn=RoundWrk.temp&dataOut, dataOut=RoundWrk.temp&dataOut);

		%GetDistance(workLib=&worklib, dataIn=RoundWrk.temp&dataOut ,dist=Distance);
		%GetInfoLoss(workLib=&worklib, dataIn=RoundWrk.temp&dataOut ,dist=InfoLoss);

		/*&distance lt &TotalDistance and *&distance lt &TotalDistance and */
		%if (&DistMax eq 1) and ( &distance lt &totalDistance) %then %do; 
	 		%let TotalDistance= &distance;
			%let totalInfoLoss= &infoLoss;
			data _null_;
				put @8 "Replicate: &MultiCount" @28 "Iterations: &ITACount" @48 "Distance: &Distance" @68 "InfoLoss: &infoloss"  ;
			run;

			%UpdateOutput(workLib=&worklib, dataIn=RoundWrk.temp&dataOut, dataOut=&dataOut);
		%end; 
		%else %if (&DistMax eq 0) and ( &infoLoss lt &totalInfoLoss) %then %do; 
			%let TotalDistance= &distance;
			%let totalInfoLoss= &infoLoss;
			data _null_;
				put @8 "Replicate: &MultiCount" @28 "Iterations: &ITACount" @48 "Distance: &Distance" @68 "InfoLoss: &infoloss"  ;
			run;

			%UpdateOutput(workLib=&worklib, dataIn=RoundWrk.temp&dataOut, dataOut=&dataOut);
		%end;

		%let MultiCount=%eval(&MultiCount+1);
	%end;

%mend MultiRound;

%macro SQRoundLP(workLib=, DataIn=, Base=, DataOut= ) /store ;
/*Performs sequential rounding*/
	%local invar;
	%let invar=rnd&var;
	/*Prepare file containing margin information*/
	proc sort data=&workLib..Outcell; by cellId;run;
	data &workLib..outCell(keep=MarginId _MarginTotal _OriginalTotal);
		merge &workLib..outcell(in=inA) &DataIn(in=inb keep=cellId &invar &var);
		by cellId;
		if inA;
		MarginId=cellId;
		_OriginalTotal=&var;
		_MarginTotal=&invar;
	run;

	data &workLib..inCellMarginMap;
		set &workLib..inCellMargin(rename=(cellId=marginId incell=cellId ));
	run;

	/*Order variables*/
	/*Calculate residuals*/
	proc sort data=&DataIn; by cellId;run;
	proc sort data=&workLib..incell; by cellId;run;
	data &workLib..InCell(drop= res upres);
		merge &workLib..incell(in=inA) &DataIn(in=inb keep=cellId &invar);
		by cellId;
		if inA;
		lowres	= mod(&invar,&base);
		if lowres= 0 then upres=0;
		else upres=&base-lowres;
		res= min(lowres,upres);
		order=res;
	run;
	proc sort data=&workLib..InCell ; by order;run;
	%let Ncell=%Nobs(&workLib..Incell);

	%let hashSizeIn = %sysfunc(ceil(%sysevalf(%sysfunc(log(%sysevalf(%Nobs(&worklib..inCellMarginMap)/1000))) / %sysfunc(log(2)))));
		/*%put HashSize : &hashSizeIn;*/
	%let hashSizeOut = %sysfunc(ceil(%sysevalf(%sysfunc(log(%sysevalf(%Nobs(&worklib..outCell)/1000))) / %sysfunc(log(2)))));
		/*%put HashSize : &hashSizeOut;*/

	/*Sequential Rounding*/
	data &workLib..InCell(keep=cellId rnd&invar);
		if _N_=1 then do;
		put;
		put @4 "Sequential Rounding";
		declare hash inCellMargin(dataset:"&worklib..inCellMarginMap" , multidata: 'y'); /* , hashexp : &hashSizeIn);*/
			inCellMargin.defineKey('CellId');
			inCellMargin.defineData('MarginId');
			inCellMargin.defineDone();
		 	call missing (CellId, MarginId);
		declare hash outCell(dataset:"&worklib..outCell"); /* , hashexp : &hashSizeOut);*/
			outCell.defineKey('MarginId');
			outCell.defineData('MarginId','_MarginTotal','_OriginalTotal');
			outCell.defineDone();
			call missing (MarginId, _MarginTotal, _OriginalTotal);
		end;

		/*for each interior cell of the table*/
		set &workLib..InCell(keep=cellId &invar lowres ) end=eof;	

		/*Retreive associated margins*/
		rc= inCellMargin.find();
		if (rc=0) then do;
			
			/*on obtient les marges associés et on accumule la distance pour ces marges, 
			une distance qui correspond à un arrondissement vers le bas, l autre distance pour un arrondissement vers le haut*/
			outcell.find();
			distLow =abs(_MarginTotal-lowres			-_OriginalTotal);
			distUp  =abs(_MarginTotal-lowres+ &base 	-_OriginalTotal);

			/*While there are other margins*/
			incellMargin.has_next(result: r);
			do while ( r ne 0 );
				incellMargin.find_next();
				incellMargin.has_next(result: r);
				outcell.find();
				distLow = 	sum(distLow,	abs(_MarginTotal-lowres 		-_OriginalTotal));
				distUp = 	sum(distUp, 	abs(_MarginTotal-lowres + &base -_OriginalTotal));			
			end;

			/*If distance for down-rounding is lower then:*/	
			if (distLow<distUp) then do;	
			
				/*Update margins*/
				_MarginTotal= _MarginTotal-lowres;
				outcell.replace();	
				incellMargin.has_prev(result: r);
				do while ( r ne 0 );
					incellMargin.find_prev();
					incellMargin.has_prev(result: r);
					outcell.find();	
					_MarginTotal= _MarginTotal-lowres;
					outcell.replace();	
				end;
				/*Round interior cell*/
				rnd&invar=&invar -lowres;

			end;
			else do;/*alors distLow>=distUp, donc on arrondie vers le haut*/

				/*Update margins*/
				_MarginTotal= _MarginTotal+ (&base-lowres);
				outcell.replace();
				incellMargin.has_prev(result: r);
				do while ( r ne 0 );
					incellMargin.find_prev();
					incellMargin.has_prev(result: r);
					outcell.find();	
					_MarginTotal= _MarginTotal+ (&base-lowres);
					outcell.replace();	
				end;
				/*Round interior cell*/
				rnd&invar=&invar + (&base-lowres);
			end;
	
		end;
		
		if eof then do;
			outcell.output(dataset:"&worklib..outCell(rename=(MarginId=cellId _MarginTotal=rnd&var _OriginalTotal=&var))");
		end;
	run;

	/*Retreive results*/
	proc sort data=&workLib..incell; by cellId;run;
	data &dataOut(keep=cellId &var rnd&var );
		update &dataIn &workLib..incell(rename=(rnd&invar=rnd&var));
		by cellId;
	run;
	proc sort data=&workLib..outCell; by cellId;run;
	data &dataOut(keep=cellId &var rnd&var);
		update &dataOut &worklib..outCell;
		by cellId;
	run;

%mend SQRoundLP;

%macro RoundLP(workLib=, DataIn=,  ConsIn=, Base= ,DataOut= ) /store;
/*Performs controlled rounding/

	/*Read constraints*/
	proc sql;
		create table &workLib..ConsName  as
		select distinct ConsID
		from &ConsIn;
	quit;
		
	%let Ncell=%Nobs(&workLib..Incell);

	/*Table rounded to the closest base*/
	data &workLib..RoundTable;
		set &DataIn end=eof;
		lowres	= mod(&var,&base);
		if lowres = 0 then upres =0;
		else upres = &base- lowres;
		res		= min(lowres,upres);
		cost	= upres - lowres;
		if eof then call symputx('nbCells',_N_);
	run;
	
	/*Rounding*/
	proc optmodel PRINTLEVEL=0 ;
		/*Declare variables*/
		set allCell ;
		set inCell ;
		set outcell;
		set Constraint ;
		set <num,num> ConsCellID ;

		num OriginalTable{allCell};
		num upres{allCell};
		num lowres{allCell};
		num Cost{inCell};
		num Fixed{inCell};
		num ConstraintCoeff{Constraint, allCell};

		num NFixed;
		var Rounding{allCell} %if (%upcase(&Solve)=MILP)%then %do; binary %end; %else %do; init 0 >= 0 <= 1  %end; ;

		/*Read data*/
		read data &workLib..inCell 		into 	inCell=[CellID] ;
		read data &workLib..outCell 	into 	outCell=[CellID] ;
		read data &workLib..ConsName 	into 	Constraint=[ConsID] ;
		read data &workLib..RoundTable 	into 	allCell=[CellID] OriginalTable=&var cost=cost upres=upres lowres=lowres;
		read data &ConsIn 	into 	ConsCellID=[ConsID  CellID] ConstraintCoeff=Coefficient;

		/*If some cells are already rounded, keep them unchanged*/
		for {i in inCell} rounding[i]= lowres[i]/&base;
		for {i in allCell} if min(lowres[i],upres[i])=0 then do; Rounding[i] =0; fix Rounding[i] =0; end;
		
		/*Margins*/
		impVar RndCell{i in allCell} = (&Base*Rounding[i] + OriginalTable[i]- lowres[i]) ;

		con Q { k in Constraint}: sum{ <r,i> in  ConsCellID : r=k}  ConstraintCoeff[k,i]* (&Base *Rounding[i]  + OriginalTable[i]- lowres[i] ) =0;

		min LinearDistance = sum{i in inCell} cost[i]*Rounding[i] ;

		/*Resolve */
		solve with &Solve %if (&MaxTime ne ) %then %do; /  TIMETYPE=1 MAXTIME=%sysevalf(&Maxtime*60) %end; ; 

		for {i in inCell} do;
				rounding[i]=roundz(rounding[i],1e-12);	
				if ((rounding[i] eq 0) or (rounding[i] eq 1)) then fixed[i]=1;
				else fixed[i]=0;
		end;

		NFixed = sum{i in incell} fixed[i];
		put @8 "Rounded: " NFixed "/ " "&NCell";
		call symputx('Nfixed',Nfixed);

		/*Output*/
		create data &dataOut from [cellId]=allCell rnd&var=RndCell &var=OriginalTable;
	quit;

	data &dataOut;
		set &dataOut;
		rnd&var=roundz(rnd&var,1e-12);
	run;

%mend RoundLP;

%macro TableCons(
		CellIn=, 
		by=, 
		var=, 
		ConsOut=, 
		DataOut= ) /store ;
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


	/*If an input from proc freq is provided then the constraints file must be created*/
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
	
	/*Split interior cells from margins*/
	%GetInOutCell(workLib=RoundWrk,cellIn=RoundWrk.InTable,consIn=RoundWrk.InCons);

	/*log  message*/
	%put %str(      ) Number of Cells: %Nobs(RoundWrk.InTable) ;
	%put %str(      ) Number of Interior Cells: %Nobs(RoundWrk.Incell);
	%put %str(      ) Number of Marginal Cells: %Nobs(RoundWrk.Outcell);
	%put %str(      ) Number of Constraints: %Nobs(RoundWrk.NCons);
	%put;
	
	/*Retreive classification variables*/
	proc sort data=RoundWrk.FreqTable; by cellID;run;	
	proc sort data=&dataOut; by cellID;run;
	data &dataOut;
		merge &dataOut RoundWrk.FreqTable(drop=&Var);
		by cellId;
	run;
	
	proc sort data=&dataOut; by cellId;run;


	/*------------Exit------------*/

	/*Clean up*/
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

%macro CtrlRound(
		CellIn=, 
		ConsIn=,
		by=, 
		var=, 
		base=, 
		adjust=, 
		multiStart=,
		seed=1,
		distMax=1,
		solve=noLP,
		radius=1,
		ctrl=NO,
		maxTime=,
		DataOut=) /store;
/*Called by a user to perform controlled rounding*/
	%put;
	%put -------------------;
	%put Controlled Rounding;
	%put -------------------;
	%put;

	%local startTime options adjustAll TotalDistance TotalInfoLoss ITACount pathOfWork workout Nfixed addrate ReMargins;
	%let StartTime = %Time();
	%let options = %saveOptions();
	options nonotes nomprint;


	/*-------------Check Input and Init Variables-------------*/
	%if (&var=) %then %do;
		%put ERROR: var= cannot be missing;
		%goto exit;
	%end;
	%if (&base=) %then %do;
		%let Base=1;
		%put WARNING: base= missing;
		%put WARNING: Default set to 1 ;
		%put;
	%end;
	%else %if (&base<=0) %then %do;
		%put ERROR: Rounding base cannot be lower than or equal to 0;
		%goto exit;
	%end;
	%if (&seed eq ) or (&seed<1) %then %do;
		%let seed=1;
	%end;
	%if ((&multiStart eq ) and (&MaxTime eq )) %then %do;
		%let multiStart=1;
	%end;
	%if (&consIn eq ) and (&by eq  ) %then %do;
		%put ERROR: Provide either ConsIn= or by=;
		%goto exit;
	%end;
	%if (&consIn ne ) and (&by ne  ) %then %do;
		%put WARNING: ConsIn= and by= were both provided;
		%put WARNING: ConsIn= &ConsIn will be used ;
		%put;
	%end;
	%if (&consIn ne ) and ( not %sysfunc(exist(&consIn)) ) %then %do;
		%put ERROR: File &consIn doesn%str(%')t exist;
		%goto exit;
	%end;
	%if (&CellIn ne ) and ( not %sysfunc(exist(&CellIn)) ) %then %do;
		%put ERROR: File &CellIn doesn%str(%')t exist;
		%goto exit;
	%end;
	%if %sysevalf(&radius < 1)  %then %do;
		%put ERROR: radius has to be greater than 0;
		%goto exit;
	%end;

	%let AdjustAll=TRUE;
	%let TotalDistance=0;
	%let addRate = 1;
		
	%if (&Adjust ne ) %then %do;
		%let dsId = %sysfunc(open(&CellIn));
		%let AdjustNum = %sysfunc(varnum(&dsid,&ADJUST));
		%let rc = %sysfunc(close(&dsId));

		%if &AdjustNum =0 %then %do;
			%put ERROR: Variable &ADJUST has to be provided in &CellIn;
			%goto exit;
		%end;
		%else %do;
			proc freq data=&cellIn noprint; table &adjust / out=_adjustFreq; run;
			%let adjustErr=0;
			data _null_;
				set _adjustFreq;
				if (&adjust ne 0 and &adjust ne 1 and &adjust ne .) then call symputx('adjustErr',1);
			run;
			proc delete data=_adjustFreq ;run;
			%if &AdjustErr = 1 %then %do;
				%put ERROR: Variable &ADJUST has to contain 0 or 1;
				%goto exit;
			%end;
			%else %do;
				%let AdjustAll=FALSE;
			%end;
		%end;
	%end;

	%if (&AdjustAll=FALSE and (%upcase(&Solve)=LP or %upcase(&Solve)=MILP) )%then %do;
		%put ERROR: cannot adjust a subset of cells and use LP or MILP solver simultaneously;
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

	%if (&consIn ne ) %then %do;
		/*si un fichier de containtes est fourni, on s assure que les aggrégats qui pourraient venir de Condid sont éliminés*/
		%CleanCons(workLib=RoundWrk,tableIn=&CellIn, consIn=&ConsIn, outCons=RoundWrk.InCons , outTable=RoundWrk.InTable);
	%end;
	%else %if (&by ne  ) %then
	%do;
		/*If an input from proc freq is provided then the constraints file must be created*/
		%BuildCons(workLib=RoundWrk, dataIn=&CellIn, ClassVarList=&by , AnalysisVar=&var, ConsOut=RoundWrk.InCons,Tableout=RoundWrk.InTable);
		%if &ReMargins eq 1 %then %do;
			%put %str(   )ERROR: No margins are allowed in the input file;
			%goto exit;
			%put %Str(   )WARNING: Margins included in the input file will not be used;
		%end;
		
	%end;

	%put %str(  ) Problem Summary;
	/*Split interior cells from margins*/
	%GetInOutCell(workLib=RoundWrk,cellIn=RoundWrk.InTable,consIn=RoundWrk.InCons);
	/*If user wants to adjust a subset of cells, this information is saved in a file */
	%MakeAdjust(workLib=RoundWrk);
	/*On crée un fichier qui associe chaque cellule intérieure à toutes les marges qui seront affectées par un changement à cette cellule*/
	%MakeIncellMargins(workLib=RoundWrk,consIn=RoundWrk.InCons);


	%put %str(      ) Number of Cells: %Nobs(RoundWrk.InTable) ;
	%put %str(      ) Number of Interior Cells: %Nobs(RoundWrk.Incell);
	%put %str(      ) Number of Marginal Cells: %Nobs(RoundWrk.Outcell);
	%put;
	

	/*------------Perform Rounding------------*/

	%if (%upcase(&solve)=LP or %upcase(&solve)= MILP) %then %do;
		%put %str(   )Solving Linear Program ;
		%RoundLP(workLib=RoundWrk, dataIn=RoundWrk.InTable, consIn=RoundWrk.InCons, Base=&Base, dataOut=&DataOut);
		/*Calculate distance*/
		%GetDistance(workLib=RoundWrk, dataIn=&dataOut ,dist=TotalDistance);
		%GetInfoLoss(workLib=RoundWrk, dataIn=&dataOut ,dist=TotalInfoLoss);

		%if (&Nfixed < %Nobs(RoundWrk.Incell)) %then %do;
			/*!!!----COMPUTE MARGINS THEN SQROUND---!!!*/
			%BuildTable(workLib=RoundWrk, dataIn=&DataOut, dataOut=&DataOut);
			%SQRoundLP(workLib=RoundWrk, dataIn=&DataOut,  Base=&Base, dataOut=&DataOut);
			/*Calcule de la distance*/
			%GetDistance(workLib=RoundWrk, dataIn=&dataOut ,dist=TotalDistance);
			%GetInfoLoss(workLib=RoundWrk, dataIn=&dataOut ,dist=TotalInfoLoss);
			data _null_;
				put @8 "Distance: &TotalDistance";
				put @8 "Info Loss: &TotalInfoLoss";
				%if &TotalDistance eq 0 %then %do; put @8 "Info Loss: &TotalInfoLoss"; %end;
			run;
		%end;
		
	%end;
	%else %do;
		/*Performs sequential rounding*/
		data _null_;
			put @4 "Sequential Rounding";
		run;
		%SQRound(workLib=RoundWrk, dataIn=RoundWrk.InTable,  Base=&Base);
		%UpdateOutput(workLib=RoundWrk, dataIn=RoundWrk.InTable , dataOut=&dataOut);
		data &dataOut;
			set &dataOut;
			if rnd&var=. then rnd&var=&var;
		run;
		/*Calculate distance*/
		%GetDistance(workLib=RoundWrk, dataIn=&dataOut ,dist=TotalDistance);
		%GetInfoLoss(workLib=RoundWrk, dataIn=&dataOut ,dist=TotalInfoLoss);
		data _null_;
			put @8 "Distance: &TotalDistance" ;
			put @8 "Info Loss: &TotalInfoLoss";
		run;
	%end;
	
	%if (&TotalDistance gt 0  and ((&MultiStart eq ) or (&MultiStart gt 0)) and ((&MaxTime eq ) or (%TimeCheck())) ) %then %do;
		/*Si la solution n est pas contrôlée et que le nombre d itération permise est plus grande que 0, alors on procède à l ajustement itératif*/
		data _null_;
			put;
			put @4 "Iterative Adjustements";
		run;
		%UpdateInput(workLib=RoundWrk, dataOut=&dataOut);
		%ITARound(workLib=RoundWrk, Base=&Base, distance=&totalDistance, infoLoss=&totalInfoLoss);
		%UpdateOutput(workLib=RoundWrk, dataIn=&dataOut, dataOut=&dataOut);
		/*Calculate distance*/
		%GetDistance(workLib=RoundWrk, dataIn=&dataOut ,dist=TotalDistance);
		%GetInfoLoss(workLib=RoundWrk, dataIn=&dataOut ,dist=TotalInfoLoss);
		data _null_;
			put @8 "Replicate: 1" @28 "Iterations: &ITAcount" @48 "Distance: &TotalDistance" @68 "InfoLoss: &Totalinfoloss" ;
		run;
	%end;
	%if (&TotalDistance gt 0 and ((&MultiStart eq ) or (&MultiStart gt 1 ))  and ((&MaxTime eq ) or (%TimeCheck())) ) %then %do;
		/*If solution is not controlled and the number of iterations allowed is greater than 0 then procede to iterative adjustment*/
		%MultiRound(workLib=RoundWrk, Base=&Base,  dataOut=&DataOut);
		
	%end;

	%if %UPCASE(&CTRL) eq YES %then %do;
		%AddCtrl(DataIn=&dataOut, ConsIn=RoundWrk.InCons , var=&var, base= &base, DataOut=&dataOut);
	%end;

	/*Calculate distance*/
	%GetDistance(workLib=RoundWrk, dataIn=&dataOut ,dist=TotalDistance);
	%GetInfoLoss(workLib=RoundWrk, dataIn=&dataOut ,dist=TotalInfoLoss);


	/*------------Exit------------*/
	data _null_;
		put;
		put @4 "Distance: &TotalDistance";
		put @4 "Info Loss: &TotalInfoLoss";
		put @4 "Additivity Rate: &addRate"; 
	run;

	/*Retreive classification variables*/
	%if (&by ne  and &var ne and &consIn eq ) %then %do; 
		proc sort data=RoundWrk.FreqTable; by cellID;run;	
		proc sort data=&dataOut; by cellID;run;
		data &dataOut;
			merge &dataOut RoundWrk.FreqTable(drop=&Var);
			by cellId;
		run;
	%end;	

	proc sort data=&dataOut; by cellId;run;

	/*Print solution*/
	%put ; 
	%if (&TotalDistance eq 0) %then %put %str(  ) Controlled ;
	%if (&addRate eq 1) %then %put %str(  ) Additive;
	%if (&TotalDistance gt 0) %then %put %str(  ) Not Controlled ;
	%if (&addRate lt 1) %then %put %str(  ) Not Additive;

	
	/*Clean up*/
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
%mend CtrlRound;

