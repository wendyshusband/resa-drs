package resa.shedding.example;

import org.apache.commons.math3.fitting.PolynomialCurveFitter;
import org.apache.commons.math3.fitting.WeightedObservedPoints;
import org.javatuples.Pair;
import resa.shedding.basicServices.api.LearningModel;

import java.util.List;

/**
 * Created by kailin on 27/3/17.
 */
public class PolynomialRegression extends LearningModel {

    public static void main(String[] args) {
        PolynomialRegression polynomialRegression = new PolynomialRegression();
        polynomialRegression.FitTest();
    }

    private void FitTest() {
        final WeightedObservedPoints obs = new WeightedObservedPoints();
        obs.add( 0.1,0.66);
        obs.add( 0.2,0.56);
        obs.add( 0.3,0.44);
        obs.add( 0.4,0.35);
        obs.add( 0.5,0.24);
        obs.add( 0.6,0.13);
        obs.add( 0.7,0.07);
        obs.add( 0.8,0.02);
        obs.add( 0.9,0.01);
        obs.add( 0.0,1.0);
        //obs.add(0,0);obs.add(0,0);obs.add(0,0);obs.add(0,0);obs.add(0,0);obs.add(0,0);
        // Instantiate a third-degree polynomial fitter.
        final PolynomialCurveFitter fitter = PolynomialCurveFitter.create(1);

        // Retrieve fitted parameters (coefficients of the polynomial function).
        final double[] coeff = fitter.fit(obs.toList());
        for(int i=0;i<coeff.length;i++)
            System.out.println(coeff[i]);
    }

    @Override
    public double[] Fit(Object[] objects) {
        List list = (List) objects[0];
        int order = (int) objects[1];
        boolean three = (boolean) objects[2];
        final WeightedObservedPoints obs = new WeightedObservedPoints();
        //System.out.println("list.size = "+list.size());
        for(int i=0; i<list.size();i++) {
            if (three) {
                System.out.println("pair0 in: " + ((Pair<Double, Double>) list.get(i)).getValue0());
                System.out.println("pair1 out: " + ((Pair<Double, Double>) list.get(i)).getValue1());
            }
            if (!three) {
                obs.add(((Pair<Double, Double>) list.get(i)).getValue0() / 100, ((Pair<Double, Double>) list.get(i)).getValue1() / 100);
            } else {
                obs.add(((Pair<Double, Double>) list.get(i)).getValue0(), ((Pair<Double, Double>) list.get(i)).getValue1());
            }
        }
        if (three) {
            System.out.println("buildddddddddddddddddddddddddddddd~~~~~~~~~~~~~~~~~~~~~~~");
        }
        obs.add(0,0);obs.add(0,0);obs.add(0,0);obs.add(0,0);obs.add(0,0);obs.add(0,0);
        obs.add(0,0);obs.add(0,0);obs.add(0,0);obs.add(0,0);obs.add(0,0);obs.add(0,0);
        obs.add(0,0);obs.add(0,0);obs.add(0,0);obs.add(0,0);obs.add(0,0);obs.add(0,0);
        // Instantiate a third-degree polynomial fitter.
        final PolynomialCurveFitter fitter = PolynomialCurveFitter.create(order);

        // Retrieve fitted parameters (coefficients of the polynomial function).
        final double[] coeff = fitter.fit(obs.toList());
        return coeff;
    }
}
