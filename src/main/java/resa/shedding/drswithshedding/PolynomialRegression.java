package resa.shedding.drswithshedding;

import org.apache.commons.math3.fitting.PolynomialCurveFitter;
import org.apache.commons.math3.fitting.WeightedObservedPoints;
import org.javatuples.Pair;
import java.util.List;

/**
 * Created by kailin on 27/3/17.
 */
public class PolynomialRegression extends ILearningSelectivity {

    public static void main(String[] args) {
        //PolynomialRegression polynomialRegression = new PolynomialRegression();
        //polynomialRegression.FitTest();
    }

    @Override
    public double[] Fit(Object[] objects) {
        List list = (List) objects[0];
        int order = (int) objects[1];
        final WeightedObservedPoints obs = new WeightedObservedPoints();
        for(int i=0; i<list.size();i++){
            System.out.println(((Pair<Double,Double>)list.get(i)).getValue0()+((Pair<Double,Double>)list.get(i)).getValue0().getClass().toString());
            System.out.println(((Pair<Double,Double>)list.get(i)).getValue1()+((Pair<Double,Double>)list.get(i)).getValue1().getClass().toString());
            obs.add(((Pair<Double,Double>)list.get(i)).getValue0(),((Pair<Double,Double>)list.get(i)).getValue1());
        }
        // Instantiate a third-degree polynomial fitter.
        final PolynomialCurveFitter fitter = PolynomialCurveFitter.create(order);

        // Retrieve fitted parameters (coefficients of the polynomial function).
        final double[] coeff = fitter.fit(obs.toList());
        return coeff;
    }
}
