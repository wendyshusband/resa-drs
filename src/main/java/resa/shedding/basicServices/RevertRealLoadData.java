package resa.shedding.basicServices;



import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by kailin on 27/3/17.
 *
 * history lambda data,for calculate the real load.
 */

public final class RevertRealLoadData {

    private String componentId;
    private Map<String,Double> proportion;
    private ArrayList<Double> selectivityFunction;
    //private Integer type;
    private Double realLoadOUT;
    private Double realLoadIN;

    public Double getRealLoadIN() {
        return realLoadIN;
    }

    public void setRealLoadIN(Double realLoadIN) {
        this.realLoadIN = realLoadIN;
    }


    public Double getRealLoadOUT() {
        return realLoadOUT;
    }

    public void setRealLoadOUT(Double realLoadOUT) {
        this.realLoadOUT = realLoadOUT;
    }

    public RevertRealLoadData(String componentId){
        this.componentId = componentId;
        this.proportion = new HashMap<>();
        this.selectivityFunction = new ArrayList<>();
        //this.type = 0;
        this.realLoadOUT = -1.0;
        this.realLoadIN = -1.0;
    }

    public String getComponentId() {
       return componentId;
   }

    public Map<String, Double> getProportion() {
       return proportion;
   }

   public ArrayList<Double> getSelectivityFunction() {
       return selectivityFunction;
   }

   public void addProportion(String componentId,Double proportion){
       double tempCount = proportion;
       if(this.proportion.containsKey(componentId)) {
            tempCount += this.proportion.get(componentId);
       }
       this.proportion.put(componentId,tempCount);
   }

   public void addCoeff(double[] coeff){
       for(int i=0;i<coeff.length;i++){
           this.selectivityFunction.add(coeff[i]);
       }
   }

    @Override
    public String toString() {
        return "componentID="+componentId
                +" proportion="+proportion.toString()
                +" selectivityFunction="+selectivityFunction.toString()
                +" realLoadOUT="+realLoadOUT
                +" realLoadIN="+realLoadIN;
    }

    public void clear(){
        this.selectivityFunction.clear();
        this.realLoadIN = -1.0;
        this.realLoadOUT = -1.0;
        //this.type = 0;
        this.proportion.clear();
    }
    public void clearProportion(){
        this.proportion.clear();
    }
}

