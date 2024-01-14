/*Controlled Rounding Examples*/


/*Load the macro in memory*/
LIBNAME CtrlRnd "F:\\CtrlRound" ; /* insère le chemin ou se trouve ton fichier sasmacr.sas7bcat */
OPTIONS MSTORED SASMSTORE = CtrlRnd ;

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
	table var1*var2*var3/ out=myfreq sparse;  
run;


/*Simple Call*/
%CtrlRound(	CellIn=myfreq, 
			by= var1 var2 var3,
  			Var= count,
			base=5,
			MultiStart=,
			MaxTime=,
			Solve=lp,
			ctrl=no,
			DataOut=rndTable);



/*ou bien en utilisant les contraintes*/
%TableCons(
			CellIn= myfreq, 
			by= var1 var2 var3,  
			var=count,
			ConsOut= myCons, 
			DataOut= mynewTable );

/*Simple Call*/
%CtrlRound(	CellIn=mynewTable, 
			consIn=myCons,
  			Var= count,
			base=5,
			MultiStart=,
			MaxTime=,
			Solve=lp,
			ctrl=no,
			DataOut=rndTable);



		
	
			
