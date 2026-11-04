package com.excelacom.jdiameter.framework.scenarios;

import java.io.FileNotFoundException;

import org.junit.Test;
import com.excelacom.jdiameter.framework.TestingFramework;

public class InsufficientBalanceTest
{

  @Test
  public void runTest() throws FileNotFoundException
  {
    TestingFramework tF = new TestingFramework();
    
    tF.executeTest( this.getClass().getClassLoader().getResourceAsStream( "scenarios/insufficient-balance-test.xml" ) );
  }
  
}
