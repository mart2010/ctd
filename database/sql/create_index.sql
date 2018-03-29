

-------------------------------------- Index  -------------------------------------

-- the goal here is to add indexes for Query optimization.

-------------------------------------------------------------------------------------------

create index review_widx on integration.review (work_refid);

create index on presentation.review (reviewer_id);
create index on presentation.review (book_id);




