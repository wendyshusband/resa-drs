package resa.util;

import resa.util.TopologyHelper;

/**
 * Created by kailin on 6/3/17.
 */
public class TestPrint{

    String _string;
    Object _object;
    public TestPrint(Object object){
        this("param=",object);
    }

    public TestPrint(String string, Object object){
        _string=string;
        _object=object;
        System.out.println(_string+_object);
    }

}
