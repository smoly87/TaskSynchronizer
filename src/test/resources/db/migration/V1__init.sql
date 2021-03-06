DROP TABLE IF EXISTS Task_Definition;
CREATE TABLE Task_Definition(
  ID INT  PRIMARY KEY,
  Name VARCHAR(255) NOT NULL,
  Description VARCHAR(255),
  LAST_MODIFIED	 TIMESTAMP DEFAULT  CURRENT_TIMESTAMP() 
);
CREATE INDEX IF NOT EXISTS  IDXLAST_MODIFIED ON Task_Definition(LAST_MODIFIED);

DROP TABLE IF EXISTS Task_Definition_Mirror;
CREATE TABLE Task_Definition_Mirror(
  ID INT PRIMARY KEY,
  Name VARCHAR(255) NOT NULL,
  Description VARCHAR(255),
  LAST_MODIFIED	 TIMESTAMP DEFAULT  CURRENT_TIMESTAMP() 
);
CREATE INDEX IF NOT EXISTS  IDXLAST_MODIFIED ON Task_Definition_Mirror(LAST_MODIFIED);