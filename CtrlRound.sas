
/*-------------------------*/
/* Arrondissement contr�l� */		
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
/*nettoye les contraintes associ�es avec les agr�gats artificiels produits par G-CONFID*/
	%local dsId vtype rc consIdnum renamecons;
	%let dsId = %sysfunc(open(&TableIn));
	%let vtype = %sysfunc(varnum(&dsId,type));
	%let rc= %sysfunc(close(&dsId));


	%let dsid = %sysfunc(open(&ConsIn));
	%let consIdnum = %sysfunc(varnum(&dsId,consId));
	%if &consIdnum=0 %then %let renameCons = ;
	%else %let renameCons = (rename=(consId=constraintId));/*j ai utilis� consId plutot que constraintId */
	%let rc = %sysfunc(close(&dsId));
	data &consIn;
		set &consIn &renameCons;
	run;

	/*si par hasard la variable type ne se trouve pas sur le tableau d entr�e, alors on ne fait rien*/
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
		/*�liminer les agr�gats artificiels du tableau*/
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
			/*on obtient les constraintes associ�es � ces agr�gats puis on les �limine du fichier des contraintes*/
			proc sql;
				create table &workLib..consAggregates as
				select distinct constraintId
				from &consIn &renameCons as c , &workLib..aggregates as a
				where c.cellid = a.cellId;
			quit;


			/*on garde les enregistrements qui on une contrainte que l on ne retrouve pas dans le fichier pr�c�dent*/
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
/*Cette macro cr�e le fichier de constraintes. */
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

	/*Filtrer les totaux*/

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


	/*  on calcul les totaux pour toutes les combinaisons de variables  */
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

	/*cr�ation du fichier des contraintes*/
	%let NCell= %Nobs(&workLib..FreqTable);
	%let consIdStart=0;
	%do i = 1 %to &Dimensions;
		
		/*fichier qui associe une marge avec les cellules � laquelle elles s aggr�gent*/
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
		Un enregistrement par contrainte est cr�� dans lequel on place la marge de cette contrainte.
		Un fichier contient les marges un autre contient les cellules qui s aggr�gent � cette marge*/
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

		/*fusion des marges et des cellules qui s y aggr�gent*/
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
/*S�pare les cellules int�rieures des marges */

	/*les marges sont faciles � identifier*/
	proc sql;
		create table &workLib..OutCell as
		select distinct CellID
		from &consIn 
		where coefficient=-1;
	quit;

	/*les cellules int�rieures sont celles qui ne sont pas des marges!*/
	proc sort data= &CellIn; 			by cellId;run;
	proc sort data= &workLib..OutCell; 	by cellId;run;
	data &workLib..InCell;
		merge &CellIn(in=inA keep=cellId) &workLib..OutCell(in=inB);
		by cellId;
		if InA and not InB;
	run;

%mend GetInOutCell; 

%macro MakeAdjust(workLib=) /store;
/*si l usager ne veut arrondir qu une partie des cellules alors on cr�e un fichier a cet effet.
	Seule les cellules int�rieures sont consid�r�es*/
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
		/*On d�mare avec les cellules int�rieure comme cellules courantes*/
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
			/*on obtient les identificateurs de contrainte des cellules courantes*/
			proc sql;
				 create table &workLib..ToConsId&i as
				 select a.incell, b.consId
				 from &workLib..ToMarginId&i as a, &workLib..InConstraints as b
				 where a.cellId=b.cellId ;
				 /*order by a.incell;*/
			quit;
			%let continue=0;

			/*on r�duit le fichier contenant les cellules InCons*/
			/*data &workLib..InConstraints;
				merge &workLib..InConstraints(in=inA) &workLib..ToConsId&i(in=inB keep=incell rename=(incell=cellId));
				by cellId;
				if inA and not inB;
			run;*/

			%if &sqlobs>0 %then %do;
				%let i = %eval(&i+1);
				%let continue=1;

				/*avec les identificateurs de contrainte des cellules courantes, on va chercher les marges auxquelle cette cellule s aggr�ge*/
				/*Les marges deviendront les nouvelles cellules courantes de la prochaine it�ration*/
				proc sql;
					create table &workLib..ToMarginId&i as
					select a.incell, b.cellId
					from &workLib..ToConsId%eval(&i-1) as a, &workLib..OutConstraints as b
					where a.consId=b.consId ;
				quit;

				/*on enregistre ces pairs*/
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
/*ajuste le fichier de sortie pour s assurer qu il soit controll�*/

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
	/*pr�paration du fichier contenant l information des marges*/

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

		/*pour chaque cellule int�rieure du tableau*/
		set &workLib..InCell(keep=cellId &var &invar) end=eof;	

		/*trouver les marges associ�es*/
		rc= inCellMargin.find();
		if (rc=0) then do;
			
			/*on obtient les marges associ�s et on accumule la distance pour ces marges, 
			une distance qui correspond � un arrondissement vers le bas, l autre distance pour un arrondissement vers le haut*/
			outcell.find();

			/*met � jour les marges*/
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
	

	/*r�cup�ration des r�sultats*/
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
/*Calcul la distance de tout un tableau*/

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
/*Calcul la perte d information de tout un tableau*/

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
/*Cette macro proc�de � l arrondissement S�quentiel */

	/*%let sqStart = %time();*/
	/*Si l arrondie ne doit etre fait que sur un sous ensemble*/
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
	

	/*pr�paration du fichier contenant l information des marges*/
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


	/*mettre les variables en ordre*/
		/*Calcul des r�sidus*/
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

		/*pour chaque cellule int�rieure du tableau*/
		set &workLib..InCell(keep=cellId &var lowres ) end=eof;	

		/*trouver les marges associ�es*/
		rc= inCellMargin.find();
		if (rc=0) then do;
			
			/*on obtient les marges associ�s et on accumule la distance pour ces marges, 
			une distance qui correspond � un arrondissement vers le bas, l autre distance pour un arrondissement vers le haut*/
			outcell.find();
			distLow =abs(_MarginTotal-_OriginalTotal-lowres);
			distUp =abs(_MarginTotal-_OriginalTotal+ (&base-lowres));

			/*tant qu il y a d autres marges*/
			incellMargin.has_next(result: r);
			do while ( r ne 0 );
				incellMargin.find_next();
				incellMargin.has_next(result: r);
				outcell.find();
				distLow = 	sum(distLow, 	abs(_MarginTotal-_OriginalTotal-lowres));
				distUp = 	sum(distUp, 	abs(_MarginTotal-_OriginalTotal+ (&base-lowres)));			
			end;

			/*Si la distance correspondant � un arrondissement vers le bas est plus faible, alors:*/	
			if (distLow<distUp) then do;	
			
				/*met � jour les marges*/
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
				/*on arrondie la cellule int�rieure*/
				rnd&var=&var -lowres;

			end;
			else do;/*alors distLow>=distUp, donc on arrondie vers le haut*/

				/*met � jour les marges*/
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
				/*on arrondie la cellule int�rieure*/
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
/*Cette macro proc�de � l arrondissement gr�ce � une m�thode d ajustements it�ratifs  */

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
		
		/*faire tant qu il y a am�lioration */
		do until (_improvement=0  or _ctrl=1 or _alarm>=&NCell);
			_improvement=0;
			/*passer � travers toutes les cellules*/
			do i = 1 to &NCell;
				/*on va chercher une cellule int�rieure*/
				modify &workLib..InCell(keep=cellId &var rnd&var) point=i;
				retain _prevCell;

				/*on identifie les marges qui y sont associ�es, il y en a au moins une*/
				rc= inCellMargin.find();
				if (rc=0 and rnd&var ne &var) then do;
					/*on va chercher la valeur de cette marge*/
					outcell.find();
					incellMargin.has_next(result: r);
					if (rnd&var > &var) then do; 
						_round= -&base;
					end;
					else do;
						_round= &base ; 
					end;

					/*on calcule la distance actuelle*/
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

					/*ainsi que la nouvelle distance obtenue si l on fait une modification*/
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

					/*on fait la m�me chose pour toutes les autres marges associ�es � cette cellule int�rieure*/
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
						
						/*on accumule toutes ces distances*/
						OldDistMax =	max(OldDistMax, 	_dist);
						OldDistSum =	sum(OldDistSum, 	_dist);
						NewDistMax = 	max(NewDistMax, 	_newdist );
						NewDistSum = 	sum(NewDistSum, 	_newdist );

						_OldInfoLoss = 	sum(_OldinfoLoss, 	_infoLossOld );
						_NewInfoLoss = 	sum(_NewinfoLoss, 	_infoLossNew );
					end;

					/*Si la nouvelle distance est meilleure que l ancienne, alors �a vaut la peine de faire un changement � cette cellule int�rieure*/
					if (%if (&DistMax eq 1) %then %do; NewDistSum<=OldDistSum  /*and NewDistMax<=OldDistMax*/ %end; %else %do; _NewInfoLoss<=_OldInfoLoss %end;) then do; 
						_improvement=1;	
						_count=_count+1;

						totalDistance=TotalDistance - roundz((OldDistSum-NewDistSum)/&base,1e-3);
						totalinfoLoss=totalinfoLoss - roundz((_OldInfoLoss-_NewInfoLoss)/&base,1e-3);
						/*
						put @8 "Iteration: " _count @32 "Distance: " TotalDistance ;
						*/

						/*Si on a atteint le crit�re d arr�t, on sort de la boucle*/
						if ( abs(totalDistance)< 1e-4) then do; 
							_ctrl=1;
							i=&NCell; 
						end;
						/*pour �viter les cycles*/
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

						/*mettre � jour les marges pour refl�ter ce changement*/
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
						/*mettre � jour la cellule int�rieure*/
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
/*Cette macro proc�de � l arrondissement gr�ce � une m�thode d ajustements it�ratifs  */

	%local MultiCount Distance infoLoss ITAcount NCell;
	%let MultiCount=2;
	%let Distance= &totalDistance;
	%let infoLoss = &totalinfoLoss;

	/*r�cup�rer les r�sultats de l ajustement s�quentiel*/
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
					/*mettre � jour la cellule int�rieure*/
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
/*Cette macro proc�de � l arrondissement S�quentiel */
	%local invar;
	%let invar=rnd&var;
	/*pr�paration du fichier contenant l information des marges*/
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

	/*mettre les variables en ordre*/
	/*Calcul des r�sidus*/
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

		/*pour chaque cellule int�rieure du tableau*/
		set &workLib..InCell(keep=cellId &invar lowres ) end=eof;	

		/*trouver les marges associ�es*/
		rc= inCellMargin.find();
		if (rc=0) then do;
			
			/*on obtient les marges associ�s et on accumule la distance pour ces marges, 
			une distance qui correspond � un arrondissement vers le bas, l autre distance pour un arrondissement vers le haut*/
			outcell.find();
			distLow =abs(_MarginTotal-lowres			-_OriginalTotal);
			distUp  =abs(_MarginTotal-lowres+ &base 	-_OriginalTotal);

			/*tant qu il y a d autres marges*/
			incellMargin.has_next(result: r);
			do while ( r ne 0 );
				incellMargin.find_next();
				incellMargin.has_next(result: r);
				outcell.find();
				distLow = 	sum(distLow,	abs(_MarginTotal-lowres 		-_OriginalTotal));
				distUp = 	sum(distUp, 	abs(_MarginTotal-lowres + &base -_OriginalTotal));			
			end;

			/*Si la distance correspondant � un arrondissement vers le bas est plus faible, alors:*/	
			if (distLow<distUp) then do;	
			
				/*met � jour les marges*/
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
				/*on arrondie la cellule int�rieure*/
				rnd&invar=&invar -lowres;

			end;
			else do;/*alors distLow>=distUp, donc on arrondie vers le haut*/

				/*met � jour les marges*/
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
				/*on arrondie la cellule int�rieure*/
				rnd&invar=&invar + (&base-lowres);
			end;
	
		end;
		
		if eof then do;
			outcell.output(dataset:"&worklib..outCell(rename=(MarginId=cellId _MarginTotal=rnd&var _OriginalTotal=&var))");
		end;
	run;

	/*r�cup�ration des r�sultats*/
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
/*Cette macro proc�de � l'arrondissement contr�l� d'une table � n dimensions. */

	/*Liste des Contraintes*/
	proc sql;
		create table &workLib..ConsName  as
		select distinct ConsID
		from &ConsIn;
	quit;
		
	%let Ncell=%Nobs(&workLib..Incell);

	/*Table arrondie � la base la plus pr�s*/
	data &workLib..RoundTable;
		set &DataIn end=eof;
		lowres	= mod(&var,&base);
		if lowres = 0 then upres =0;
		else upres = &base- lowres;
		res		= min(lowres,upres);
		cost	= upres - lowres;
		if eof then call symputx('nbCells',_N_);
	run;
	
	/*Arrondissement*/
	proc optmodel PRINTLEVEL=0 ;
		/*D�clarer les variables*/
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

		/*Lire les donn�es*/
		read data &workLib..inCell 		into 	inCell=[CellID] ;
		read data &workLib..outCell 	into 	outCell=[CellID] ;
		read data &workLib..ConsName 	into 	Constraint=[ConsID] ;
		read data &workLib..RoundTable 	into 	allCell=[CellID] OriginalTable=&var cost=cost upres=upres lowres=lowres;
		read data &ConsIn 	into 	ConsCellID=[ConsID  CellID] ConstraintCoeff=Coefficient;

		/*Si certaines cellules sont d�j� arrondie dans la table originale, garder les valeurs originales.*/
		for {i in inCell} rounding[i]= lowres[i]/&base;
		for {i in allCell} if min(lowres[i],upres[i])=0 then do; Rounding[i] =0; fix Rounding[i] =0; end;
		
		/*Marges*/
		impVar RndCell{i in allCell} = (&Base*Rounding[i] + OriginalTable[i]- lowres[i]) ;

		con Q { k in Constraint}: sum{ <r,i> in  ConsCellID : r=k}  ConstraintCoeff[k,i]* (&Base *Rounding[i]  + OriginalTable[i]- lowres[i] ) =0;

		min LinearDistance = sum{i in inCell} cost[i]*Rounding[i] ;

		/*R�soudre */
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
/*Cette macro est celle qui est appell�e par l utilisateur pour proc�der � l arrondissement*/
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


	/*Si un fichier de proc freq est fourni, alors il faut cr�er le fichier des contraintes*/
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
	
	/*S�pare les cellules int�rieures des marges*/
	%GetInOutCell(workLib=RoundWrk,cellIn=RoundWrk.InTable,consIn=RoundWrk.InCons);

	/*log  message*/
	%put %str(      ) Number of Cells: %Nobs(RoundWrk.InTable) ;
	%put %str(      ) Number of Interior Cells: %Nobs(RoundWrk.Incell);
	%put %str(      ) Number of Marginal Cells: %Nobs(RoundWrk.Outcell);
	%put %str(      ) Number of Constraints: %Nobs(RoundWrk.NCons);
	%put;
	
	/*r�cup�rer les variables de classification*/
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
/*Cette macro est celle qui est appell�e par l utilisateur pour proc�der � l arrondissement*/
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
		/*si un fichier de containtes est fourni, on s assure que les aggr�gats qui pourraient venir de Condid sont �limin�s*/
		%CleanCons(workLib=RoundWrk,tableIn=&CellIn, consIn=&ConsIn, outCons=RoundWrk.InCons , outTable=RoundWrk.InTable);
	%end;
	%else %if (&by ne  ) %then
	%do;
		/*Si un fichier de proc freq est fourni, alors il faut cr�er le fichier des contraintes*/
		%BuildCons(workLib=RoundWrk, dataIn=&CellIn, ClassVarList=&by , AnalysisVar=&var, ConsOut=RoundWrk.InCons,Tableout=RoundWrk.InTable);
		%if &ReMargins eq 1 %then %do;
			%put %str(   )ERROR: No margins are allowed in the input file;
			%goto exit;
			%put %Str(   )WARNING: Margins included in the input file will not be used;
		%end;
		
	%end;

	%put %str(  ) Problem Summary;
	/*S�pare les cellules int�rieures des marges*/
	%GetInOutCell(workLib=RoundWrk,cellIn=RoundWrk.InTable,consIn=RoundWrk.InCons);
	/*Si l usager ne veut ajuster qu un sous ensemble de cellules, alors on place cette information dans un fichier */
	%MakeAdjust(workLib=RoundWrk);
	/*On cr�e un fichier qui associe chaque cellule int�rieure � toutes les marges qui seront affect�es par un changement � cette cellule*/
	%MakeIncellMargins(workLib=RoundWrk,consIn=RoundWrk.InCons);


	%put %str(      ) Number of Cells: %Nobs(RoundWrk.InTable) ;
	%put %str(      ) Number of Interior Cells: %Nobs(RoundWrk.Incell);
	%put %str(      ) Number of Marginal Cells: %Nobs(RoundWrk.Outcell);
	%put;
	

	/*------------Perform Rounding------------*/

	%if (%upcase(&solve)=LP or %upcase(&solve)= MILP) %then %do;
		%put %str(   )Solving Linear Program ;
		%RoundLP(workLib=RoundWrk, dataIn=RoundWrk.InTable, consIn=RoundWrk.InCons, Base=&Base, dataOut=&DataOut);
		/*Calcule de la distance*/
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
		/*on proc�de � l ajustement s�quentiel*/
		data _null_;
			put @4 "Sequential Rounding";
		run;
		%SQRound(workLib=RoundWrk, dataIn=RoundWrk.InTable,  Base=&Base);
		%UpdateOutput(workLib=RoundWrk, dataIn=RoundWrk.InTable , dataOut=&dataOut);
		data &dataOut;
			set &dataOut;
			if rnd&var=. then rnd&var=&var;
		run;
		/*Calcule de la distance*/
		%GetDistance(workLib=RoundWrk, dataIn=&dataOut ,dist=TotalDistance);
		%GetInfoLoss(workLib=RoundWrk, dataIn=&dataOut ,dist=TotalInfoLoss);
		data _null_;
			put @8 "Distance: &TotalDistance" ;
			put @8 "Info Loss: &TotalInfoLoss";
		run;
	%end;
	
	%if (&TotalDistance gt 0  and ((&MultiStart eq ) or (&MultiStart gt 0)) and ((&MaxTime eq ) or (%TimeCheck())) ) %then %do;
		/*Si la solution n est pas contr�l�e et que le nombre d it�ration permise est plus grande que 0, alors on proc�de � l ajustement it�ratif*/
		data _null_;
			put;
			put @4 "Iterative Adjustements";
		run;
		%UpdateInput(workLib=RoundWrk, dataOut=&dataOut);
		%ITARound(workLib=RoundWrk, Base=&Base, distance=&totalDistance, infoLoss=&totalInfoLoss);
		%UpdateOutput(workLib=RoundWrk, dataIn=&dataOut, dataOut=&dataOut);
		/*Calcule de la distance*/
		%GetDistance(workLib=RoundWrk, dataIn=&dataOut ,dist=TotalDistance);
		%GetInfoLoss(workLib=RoundWrk, dataIn=&dataOut ,dist=TotalInfoLoss);
		data _null_;
			put @8 "Replicate: 1" @28 "Iterations: &ITAcount" @48 "Distance: &TotalDistance" @68 "InfoLoss: &Totalinfoloss" ;
		run;
	%end;
	%if (&TotalDistance gt 0 and ((&MultiStart eq ) or (&MultiStart gt 1 ))  and ((&MaxTime eq ) or (%TimeCheck())) ) %then %do;
		/*Si la solution n est pas contr�l�e et que le nombre d it�ration permise est plus grande que 0, alors on proc�de � l ajustement it�ratif*/
		%MultiRound(workLib=RoundWrk, Base=&Base,  dataOut=&DataOut);
		
	%end;

	%if %UPCASE(&CTRL) eq YES %then %do;
		%AddCtrl(DataIn=&dataOut, ConsIn=RoundWrk.InCons , var=&var, base= &base, DataOut=&dataOut);
	%end;

	/*Calcule de la distance*/
	%GetDistance(workLib=RoundWrk, dataIn=&dataOut ,dist=TotalDistance);
	%GetInfoLoss(workLib=RoundWrk, dataIn=&dataOut ,dist=TotalInfoLoss);


	/*------------Exit------------*/
	data _null_;
		put;
		put @4 "Distance: &TotalDistance";
		put @4 "Info Loss: &TotalInfoLoss";
		put @4 "Additivity Rate: &addRate"; 
	run;

	/*r�cup�rer les variables de classification*/
	%if (&by ne  and &var ne and &consIn eq ) %then %do; 
		proc sort data=RoundWrk.FreqTable; by cellID;run;	
		proc sort data=&dataOut; by cellID;run;
		data &dataOut;
			merge &dataOut RoundWrk.FreqTable(drop=&Var);
			by cellId;
		run;
	%end;	

	proc sort data=&dataOut; by cellId;run;

	/*imprime l etat de la solution*/
	%put ; 
	%if (&TotalDistance eq 0) %then %put %str(  ) Controlled ;
	%if (&addRate eq 1) %then %put %str(  ) Additive;
	%if (&TotalDistance gt 0) %then %put %str(  ) Not Controlled ;
	%if (&addRate lt 1) %then %put %str(  ) Not Additive;

	
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
%mend CtrlRound;

