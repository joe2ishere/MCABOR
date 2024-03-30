package org.mcabor;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

public class cointergration {

	@Test
	public void testForSizeMatch() {

		Double first[] = { 1. };
		try {
			Cointegration.cointegration(first, null);
			fail("expected " + BadCointegrationParm.class.getName());
		} catch (Exception e) {
			if (e.getClass() == NullPointerException.class) {
				;
			} else
				fail("unexpected " + e.getMessage());

		}
		Double second[] = { 1., .5 };
		try {
			Cointegration.cointegration(first, second);
			fail("expected " + BadCointegrationParm.class.getName());
		} catch (Exception e) {
			if (!BadCointegrationParm.class.isInstance(e))
				fail("unexpected " + e.getMessage());
			if (e.getMessage().compareTo("array lengths must match") != 0) {
				fail("expected: array lengths must match");
			}
		}
		try {
			Double third[] = {};
			Cointegration.cointegration(third, third);
			fail("expected " + BadCointegrationParm.class.getName());
		} catch (Exception e) {
			if (!BadCointegrationParm.class.isInstance(e))
				fail("unexpected " + e.getMessage());
			if (e.getMessage().compareTo("array length (first vector) must be greater than 0") != 0) {
				fail("expected array length (first vector) must be greater than 0");
			}
		}

		try {
			Double in1[] = { 1. };
			Double in2[] = { 1. };
			Cointegration.cointegration(in1, in2);
			fail("expecting exception");

		} catch (Exception e) {

			if (!BadCointegrationParm.class.isInstance(e))
				fail("expected " + BadCointegrationParm.class.getName());
			if (e.getMessage().compareTo("array range (first vector) must not be 0") != 0) {
				fail("expected: array range (first vector) must not be 0");
			}

		}
		try {
			Double in1[] = { 1., 2. };
			Double in2[] = { 1., 1. };
			Cointegration.cointegration(in1, in2);
			fail("expecting exception");

		} catch (Exception e) {

			if (!BadCointegrationParm.class.isInstance(e))
				fail("expected " + BadCointegrationParm.class.getName());
			if (e.getMessage().compareTo("array range (second vector) must not be 0") != 0) {
				fail("expected: array range (second vector) must not be 0");
			}

		}
	}

	@Test
	public void testForReturn() {
		try {
			Double in1[] = { 1., 2. };
			Double in2[] = { 1., 2. };
			var d = Cointegration.cointegration(in1, in2);
			assertTrue(d == 0);

		} catch (Exception e) {
			fail("unexpected " + e.getMessage());
		}

		try {
			Double in1[] = { 2., -1. };
			Double in2[] = { -1., 2. };
			var d = Cointegration.cointegration(in1, in2);
			assertTrue(d == 1);

		} catch (Exception e) {
			fail("unexpected " + e.getMessage());
		}

		try

		{
			Double in1[] = { 2., -1., 2. };
			Double in2[] = { 2., -1., 0. };
			var d = Cointegration.cointegration(in1, in2);
			assertTrue(d != 0);

		} catch (Exception e) {
			fail("unexpected " + e.getMessage());
		}

		try {
			Double in1[] = { 1., 2. };
			Double in2[] = { 100., 200. };
			var d = Cointegration.cointegration(in1, in2);
			assertTrue(d == 0);

		} catch (Exception e) {
			fail("unexpected " + e.getMessage());
		}

		try {
			Double in1[] = { 1., 2., 3., 4., 5. };
			Double in2[] = { 1., 20., 300., 4000., 5000. };
			var d = Cointegration.cointegration(in1, in2);
			assertTrue(d != 0); // in my test returns 0.14

		} catch (Exception e) {
			fail("unexpected " + e.getMessage());
		}

		try {
			Double in1[] = { 1., 2., 3., 4., 5. };
			Double in2[] = { 1., -20000., 3., 4., 5e-9 };
			var d = Cointegration.cointegration(in1, in2);
			assertTrue(d < .4); // in my test returns 0.3999989985749999

		} catch (Exception e) {
			fail("unexpected " + e.getMessage());
		}

	}

}
