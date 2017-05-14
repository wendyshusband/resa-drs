package resa.shedding.drswithshedding;

import org.apache.commons.math3.fitting.PolynomialCurveFitter;
import org.apache.commons.math3.fitting.WeightedObservedPoints;
import org.javatuples.Pair;
import java.util.List;

/**
 * Created by kailin on 27/3/17.
 */
public class PolynomialRegression extends LearningSelectivity {

   /* public static void main(String[] args) {
        PolynomialRegression polynomialRegression = new PolynomialRegression();
        polynomialRegression.FitTest();
        System.out.println(
                Math.log10(644)+"a"+
                Math.log10(672)
        );
    }


    private void FitTest() {
        final WeightedObservedPoints obs = new WeightedObservedPoints();
        obs.add(Math.log10(644),Math.log10(672));
        obs.add(Math.log10(833),Math.log10(657));
        obs.add(Math.log10(655),Math.log10(655));
        obs.add(Math.log10(489),Math.log10(620));
        obs.add(Math.log10(800),Math.log10(698));
        obs.add(0,0);obs.add(0,0);obs.add(0,0);obs.add(0,0);obs.add(0,0);obs.add(0,0);
        //obs.add(0,0);obs.add(0,0);obs.add(0,0);obs.add(0,0);obs.add(0,0);obs.add(0,0);

        // Instantiate a third-degree polynomial fitter.
        final PolynomialCurveFitter fitter = PolynomialCurveFitter.create(1);

        // Retrieve fitted parameters (coefficients of the polynomial function).
        final double[] coeff = fitter.fit(obs.toList());
        for(int i=0;i<coeff.length;i++)
            System.out.println(coeff[i]);
    }*/

    @Override
    public double[] Fit(Object[] objects) {
        List list = (List) objects[0];
        int order = (int) objects[1];
        final WeightedObservedPoints obs = new WeightedObservedPoints();
        System.out.println("list.size = "+list.size());
        for(int i=0; i<list.size();i++){
            System.out.println("pair0 in: "+((Pair<Double,Double>)list.get(i)).getValue0());
            System.out.println("pair1 out: "+((Pair<Double,Double>)list.get(i)).getValue1());
            obs.add(((Pair<Double,Double>)list.get(i)).getValue0(),((Pair<Double,Double>)list.get(i)).getValue1());
        }
        System.out.println("buildddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd");
        obs.add(0,0);obs.add(0,0);obs.add(0,0);obs.add(0,0);obs.add(0,0);obs.add(0,0);
        obs.add(0,0);obs.add(0,0);obs.add(0,0);obs.add(0,0);obs.add(0,0);obs.add(0,0);
        // Instantiate a third-degree polynomial fitter.
        final PolynomialCurveFitter fitter = PolynomialCurveFitter.create(order);

        // Retrieve fitted parameters (coefficients of the polynomial function).
        final double[] coeff = fitter.fit(obs.toList());
        return coeff;
    }
}
