package com.excelacom.jdiameter.framework.scenarios;

import java.io.FileNotFoundException;

import org.junit.Test;
import com.excelacom.jdiameter.framework.TestingFramework;

public class UnknownRequestedActionTest
{

  @Test
  public void runTest() throws FileNotFoundException
  {
    TestingFramework tF = new TestingFramework();
    
    tF.executeTest( this.getClass().getClassLoader().getResourceAsStream( "scenarios/unknown-requested-action-test.xml" ) );
  }
  
}
