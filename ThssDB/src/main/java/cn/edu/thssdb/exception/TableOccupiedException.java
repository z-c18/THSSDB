package cn.edu.thssdb.exception;

public class TableOccupiedException extends RuntimeException{
    private String tablename;

    public TableOccupiedException(String tablename)
    {
        super();
        this.tablename = tablename;
    }

    @Override
    public String getMessage()
    {
        return "Table " + tablename + " is occupied!";
    }
}

