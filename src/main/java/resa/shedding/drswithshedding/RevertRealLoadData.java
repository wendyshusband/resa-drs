package resa.shedding.drswithshedding;



import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by kailin on 27/3/17.
 *
 * history lambda data,for calculate the real load.
 */

final class RevertRealLoadData {

   private String componentId;
   private Map<String,Double> proportion;
   private ArrayList<Double> selectivityFunction;
   private Integer type;
   private Double realLoadOUT;

    public Double getRealLoadIN() {
        return realLoadIN;
    }

    public void setRealLoadIN(Double realLoadIN) {
        this.realLoadIN = realLoadIN;
    }

    private Double realLoadIN;

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
       this.type = 0;
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
       this.proportion.put(componentId,proportion);
   }

   public void addCoeff(double[] coeff){
       for(int i=0;i<coeff.length;i++){
           this.selectivityFunction.add(coeff[i]);
       }
   }

    @Override
    public String toString() {
        return "componentID="+componentId+" type="+type
                +" proportion="+proportion.toString()
                +" selectivityFunction="+selectivityFunction.toString()
                +" realLoadOUT="+realLoadOUT
                +" realLoadIN="+realLoadIN;
    }

    public void setType(int type) {
        this.type = type;
    }

    public int getType() {
        return this.type;
    }

    public void clear(){
       this.selectivityFunction.clear();
       this.realLoadIN = -1.0;
       this.realLoadOUT = -1.0;
    }
}

