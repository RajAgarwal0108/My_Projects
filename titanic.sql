##################   DATA PRE-PROCESSING   #############

-- looking at first 5 dataset
select  * from titanic_original limit 5;

-- types of dataset
describe titanic_original;

select count(*) from titanic_original; #891 records

select pclass , class from titanic_original;
--  pclass is one the integer value of class therefore, we can drop class field
 
alter table titanic_original
drop column class;

select distinct(embarked) , embark_town from titanic_original; 
-- there are one 3 distinct embarked[S,C,Q] which are the abbrebiation of embark_town
-- therefore we can drop embarked field

alter table titanic_original
drop column embarked;

select distinct(survived) , alive from titanic_original;

-- fields survived and alive communicate same details, therfore dropping alive 

alter table titanic_original
drop column alive;



--------------------------------- DATA ANALYSING ----------------------------

select who,sum(survived)    -- who	sum(survived)
from                        -- man	88
	titanic_original        -- woman 205
group by                    -- child 49
	who;
    
    
    
select sex , count(sex) , 
			sum(survived)      -- total no of male : 577 , survived : 109
from                           -- total no of female : 314 , survived : 233 
	titanic_original
group by
	sex;
    
    
select age,count(age),sum(survived)
from
	titanic_original
group by
	age
order by
	age;
    
    
select * from titanic_original limit 5;

select count(adult_male) , sex
from titanic_original
where sex = 'male' and adult_male = 'False'; 

-- there are 40 male which aren't adult



select sex , count(sex) , 
			sum(survived),
		count(adult_male)      
from                           
	titanic_original
where 
	sex = 'male' and adult_male = 'True'
group by
	sex;
    
     --   sex	count(sex)	sum(survived)	count(adult_male)
	 --   male	537			88				537
     

select 
	sum(survived) , embark_town
from
	titanic_original
group by
	embark_town;

--   sum(survived)	| embark_town
--   ------------------------------
--  	217			| Southampton
--      93		    | Cherbourg
--      30			| Queenstown


select
	sum(survived) , who,alone
from
	titanic_original
group by
	who , alone;
    
    

select
	max(fare) , min(fare) , avg(fare)
from titanic_original;

-- max(fare)	min(fare)	avg(fare)
-- 	512.3292	0			32.2042079685746


-- survival rate based on number of siblings with the person

select
	sum(survived) , sibsp
from
	titanic_original
group by
	sibsp;


-- survival rate based on the size of family

    
select
	sum(survived) , parch
from
	titanic_original
group by
	parch;
   
-- people who were travelling by first class survived most   

select 
	sum(survived),pclass
from 
	titanic_original
group by
	pclass;
    
    
select 
	sum(survived),deck
from 
	titanic_original
group by
	deck;
    
    
    











    








