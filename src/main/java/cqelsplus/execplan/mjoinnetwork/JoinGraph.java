package cqelsplus.execplan.mjoinnetwork;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.log4j.Logger;

import com.hp.hpl.jena.sparql.core.Var;

import cqelsplus.engine.Config;
import cqelsplus.execplan.oprouters.MJoinRouter;
import cqelsplus.execplan.utils.OpUtils;
import cqelsplus.execplan.windows.PhysicalWindow;
import cqelsplus.execplan.windows.VirtualWindow;
import cqelsplus.execplan.windows.WindowManager;

public class JoinGraph {
	final static Logger logger = Logger.getLogger(JoinGraph.class);
	Vertex root;
	List<MJoinRouter> mjoinRouters;
	int coveredMJoinRouters = 0;
	WindowManager wm;
	PhysicalWindow startPW;
	static int psId = 0;
	public JoinGraph(List<MJoinRouter> mjoinRouters) {
		this.mjoinRouters = mjoinRouters;
		this.wm = WindowManager.getInstance();
	}
	
	public JoinGraph(List<MJoinRouter> mjoinRouters, PhysicalWindow pW) {
		this.mjoinRouters = mjoinRouters;
		this.wm = WindowManager.getInstance();
		startPW = pW;
		//logger.info("start build join graph");
		generateProbeGraph(startPW);
		//logger.info("finish build join graph");
	}	
	
	public JoinGraph(List<MJoinRouter> mjoinRouters, List<VirtualWindow> startSharedVWs) {
		this.mjoinRouters = mjoinRouters;
		this.wm = WindowManager.getInstance();
		generateProbeGraph_2(startSharedVWs);
	}	

	public Vertex getRoot() {
		return root;
	}

	/**
	 * This method generates a corresponding graph with the specified physical window
	 * @param rootW Physical window
	 */
	
	public void generateProbeGraph(PhysicalWindow rootW) {
		/**init steps*/
		for (MJoinRouter mjr : mjoinRouters) {
			mjr.setCovered(false);
		}
		
	 	for (VirtualWindow vw : wm.getVirtualWindows()) {
	 		vw.setVisited(false);
	 		vw.setBefore(null);
	 	}
	 	/**End init*/
		/**1. Create the root Vertex of the Probing Graph*/
		root = new Vertex(rootW);
		/**accumulated var position needs to be identified of which id*/
		root.initProbedInfo();
		/**2. Create the list to save the number of variables accumulated*/
		List<AccVars> al = new ArrayList<AccVars>();
		/**3. Loop throughout all of the virtual windows which is represented
		by this physical window to save the accummulating variables*/
		for (VirtualWindow vw : rootW.getVWs()) {
			if (vw.isRemoved()) continue;
			AccVars ac = new AccVars(OpUtils.parseVarsToArray(vw.getOp()));
			ac.addVisitedVWs(vw);
			ac.setVisitedMJoinRouter(vw.getItsCurrentMJoinRouter());
			al.add(ac);
	 		vw.setBefore(null);
		}
		/**4. Set accumulated variables to the vertex and look for the next vertex*/
		root.setAccVarsList(al);
		coveredMJoinRouters = 0;
		propagate(root, new ArrayList<VirtualWindow>());
		/**Supplement case: 2 virtual windows in the same query referring to same physical window */
		for (VirtualWindow vW : rootW.getVWs()) {
			MJoinRouter router = vW.getItsCurrentMJoinRouter();
			if (!router.havingSharedVirtualWindows()) continue;
			boolean alreadyStarted = router.checkStartedVW(vW);
			if (alreadyStarted) continue;
			/**Generate a probing sequence starting from this virtual window*/
			/**Init again*/
			router.setCovered(false);
			for (VirtualWindow vWInSameQ : router.getVWs()) {
				vWInSameQ.setVisited(false);
			}
			al = new ArrayList<AccVars>();
			AccVars ac = new AccVars(OpUtils.parseVarsToArray(vW.getOp()));
			ac.addVisitedVWs(vW);
			ac.setVisitedMJoinRouter(vW.getItsCurrentMJoinRouter());
			al.add(ac);
			vW.setBefore(null);
			/**End init again*/
			root.setAccVarsList(al);
			coveredMJoinRouters = 0;
			propagate(root, new ArrayList<VirtualWindow>());
		}
		root.sortAscendingProbingIndexes();
		/**Put the id for each probing sequence*/
		/**End putting the id for probing sequence*/
		if (Config.PRINT_LOG) {
			root.printLog("");
		}		
	}
 
		/**
		 * This method will search and generate a probing sequence or graph
		 * with the heuristic based on the most coverage value.
		 * On every step of finding the most coverage vertex, we loop through all of the
		 * non-visited virtual windows and consider the position of the set of variables
		 * which are shared with previous accumulated set. 
		 * @param a current vertex
		 */
		
		public void propagate(Vertex vertex, List<VirtualWindow> ps) {
			//for the special case
			boolean setBefore = false;
			for (AccVars av : vertex.getAccVarsList()) {
				if (av.isSatifiedMJoinRouter() && (!av.getVisitedMJoinRouter().isCovered())) {
					if (Config.PRINT_LOG) {
						logger.info("Trivial cases have triggered into if statement");
					}
					//save the path first
					av.getVisitedMJoinRouter().saveProbingSequence(av.getLastJoinedVW(), av.getVars(), vertex.getId());
					
					av.getVisitedMJoinRouter().setCovered(true);
					
					vertex.addSatisfiedMJoinRouter(av.getVisitedMJoinRouter());
					
					coveredMJoinRouters ++;
					if (coveredMJoinRouters == mjoinRouters.size()) {
						if (Config.PRINT_LOG) {
							logger.info("Trivial cases have affected query coverage");
						}
						return;
					}
				}
			}
			/**loop until all of the queries are covered*/
			while (coveredMJoinRouters < mjoinRouters.size()) {	
				/**each step of loop, we need to manage the max coverage value call overlapped value
				it will not be accurate if the same value is created multiple times so a manager
				of these value are needed.*/
				OVManager ovm = new OVManager();
				/**with every accumulated set of variables, check it with non-visited window in the
				same query to calculate the value of the combination of shared variable position 
				in both previous set and variables in this virtual window*/
				for (AccVars av : vertex.getAccVarsList()) {
					for (VirtualWindow vTmp : wm.getVirtualWindows()) {
						//supplement: just consider all virtual windows in the same turn of registration
						//every specific consideration is just meaningful if they are in the same query
						//test
						boolean x = (av.getVisitedMJoinRouter().getId() == vTmp.getItsCurrentMJoinRouter().getId());
						boolean y = !av.visited(vTmp);
						boolean z = (!vTmp.isVisited());
						boolean w = (!vTmp.isRemoved());
						//System.out.println("x : " + x + " y: " + y + " z: " + z);
						if (x && y && z && w) {
	
							//get the shared variable(s) of these 2 sets
							ArrayList<Var> sharedVar = OpUtils.getOverlappedKey(av.getValue(), vTmp.getOp());
							
							if (sharedVar != null) {
								
								ArrayList<Integer> next = new ArrayList<Integer>();
								ArrayList<Integer> prev = new ArrayList<Integer>();
								//build the previous and the next shared variables set
								for (int i = 0; i < sharedVar.size(); i ++) {
									
									int n = OpUtils.getVarIdx(vTmp.getOp(), sharedVar.get(i));
									int p = av.getVarIdx(sharedVar.get(i));
									
									next.add(n);
									prev.add(p);
								}
								
								OverlappedValue ov = ovm.getOV(vTmp.getPW().getId(), next, prev);
								
								if (ov == null) {
									
									ov = ovm.createOV(vTmp.getPW(), next, prev);
									
								}
								ov.addTmpNextVirtualWindow(vTmp);
								
								ov.addContainedMJoinRouter(vTmp.getItsCurrentMJoinRouter());
							}
						}
					}
				}

				
				OverlappedValue ov = ovm.getMaxOV();
				
				if (ov == null)
					return;
				
				if (ov.getWeight() > 0) {
					//init window index
					ov.getPhysicalWindow().addIndexColumns(ov.getNextVarCols());
					//vertex.getPW().addIndexes(ov.getPrevVarCols());
					//prepare a new vertex, this vertex will be the child of v
					Vertex newVertex = new Vertex(ov.getPhysicalWindow());
					vertex.addChild(newVertex);
					
					//this new vertex contains a list of new accumulated variables set
					ArrayList<AccVars> accList = new ArrayList<AccVars>();
					newVertex.setAccVarsList(accList);
					newVertex.setArrivedEdge(vertex, ov);
					
					//consider again the pair of acc variables and virtual window
					//to find out which one is a part of composition of ov
					ArrayList<ProbingInfo> newProbingInfo = null;

					BitSet flags = new BitSet();
					for (AccVars av : vertex.getAccVarsList()) {

						for (VirtualWindow chosenV : ov.getTmpNextVirtualWindow()) {
							
							if (av.getVisitedMJoinRouter().getId() == chosenV.getItsCurrentMJoinRouter().getId()
									&& flags.get(av.getVisitedMJoinRouter().getId()) == false) {
								
								ArrayList<Var> sharedVar = OpUtils.getOverlappedKey(av.getValue(), chosenV.getOp());
								//for sure this if will be true at least 1
								if (sharedVar != null) {
									
									boolean passed = true;
									
									for (int i = 0; i < sharedVar.size(); i++) {
										
										try {
										boolean x = ov.getNextVarCol(i) != OpUtils.getVarIdx(chosenV.getOp(), sharedVar.get(i));
										boolean y = ov.getPrevVarCol(i) != av.getVarIdx(sharedVar.get(i));
										if (x || y) {
											passed = false;
											break;
										}
//										if (ov.getNextVarPos(i) != OpUtils.getVarIdx(chosenV.getOp(), sharedVar.get(i)) ||
//												ov.getPrevVarPos(i) != av.getVarIdx(sharedVar.get(i))) {
//											passed = false;
//											break;
//										}
										} catch (Exception e) {
											passed = false;
											break;
										}
									}
									if (passed) {
										
										AccVars newAcc = av.clone();
										
										newProbingInfo = newAcc.addVars(chosenV, vertex);
										
										newAcc.addVisitedVWs(chosenV);
										
										accList.add(newAcc);

										newVertex.setProbingInfo(newProbingInfo);
										//save the path
										setBefore = true;
										chosenV.setBefore(av.getLastJoinedVW());
										ps.add(av.getLastJoinedVW());
										if (newAcc.isSatifiedMJoinRouter() && (!newAcc.getVisitedMJoinRouter().isCovered())) {
											//save the probing sequence information
											newAcc.getVisitedMJoinRouter().saveProbingSequence(chosenV, newAcc.getVars(), newVertex.getId());
											//newAcc.getVisitedMJoinRouter().saveProbingSequence(ps, newAcc.getVars(), newVertex.getId());
											
											newAcc.getVisitedMJoinRouter().setCovered(true);
											
											newVertex.addSatisfiedMJoinRouter(newAcc.getVisitedMJoinRouter());

											coveredMJoinRouters ++;
											if (coveredMJoinRouters == mjoinRouters.size())
												return;
										}
										//found++;
										flags.set(chosenV.getItsCurrentMJoinRouter().getId());
										break;										
									}
								}
							}
						}
//						if (found == ov.getWeight()) {
//							break;
//						}
						if (flags.cardinality() == ov.getWeight()) {
							break;
						}						
					}
					//ov.sortIndexPos();
					propagate(newVertex, ps);
					if (setBefore) {
						ps.remove(ps.size()-1);
						setBefore = false;
					}
				} else  {
					return;
				}
			}
		}
		
		private void generateProbeGraph_2(List<VirtualWindow> startSharedVWs) {
			/**init steps*/
			for (MJoinRouter mjr : mjoinRouters) {
				mjr.setCovered(false);
			}
			
		 	for (VirtualWindow vw : wm.getVirtualWindows()) {
		 		vw.setVisited(false);
		 		vw.setBefore(null);
		 	}
		 	/**End init*/
			/**1. Create the root Vertex of the Probing Graph*/
			root = new Vertex(startSharedVWs.get(0).getPW());
			/**accumulated var position needs to be identified of which id*/
			root.initProbedInfo();
			/**2. Create the list to save the number of variables accumulated*/
			List<AccVars> al = new ArrayList<AccVars>();
			/**3. Loop throughout all of the virtual windows which is represented
			by this physical window to save the accummulating variables*/
			for (VirtualWindow vw : startSharedVWs) {
				if (vw.isRemoved()) continue;
				AccVars ac = new AccVars(OpUtils.parseVarsToArray(vw.getOp()));
				ac.addVisitedVWs(vw);
				ac.setVisitedMJoinRouter(vw.getItsCurrentMJoinRouter());
				al.add(ac);
		 		vw.setBefore(null);
			}
			/**4. Set accumulated variables to the vertex and look for the next vertex*/
			root.setAccVarsList(al);
			coveredMJoinRouters = 0;
			propagate_2(root);
			/**Supplement case: 2 virtual windows in the same query referring to same physical window */
			for (VirtualWindow vW : startSharedVWs) {
				MJoinRouter router = vW.getItsCurrentMJoinRouter();
				if (!router.havingSharedVirtualWindows()) continue;
				boolean alreadyStarted = router.checkStartedVW(vW);
				if (alreadyStarted) continue;
				/**Generate a probing sequence starting from this virtual window*/
				/**Init again*/
				router.setCovered(false);
				for (VirtualWindow vWInSameQ : router.getVWs()) {
					vWInSameQ.setVisited(false);
				}
				al = new ArrayList<AccVars>();
				AccVars ac = new AccVars(OpUtils.parseVarsToArray(vW.getOp()));
				ac.addVisitedVWs(vW);
				ac.setVisitedMJoinRouter(vW.getItsCurrentMJoinRouter());
				al.add(ac);
				vW.setBefore(null);
				/**End init again*/
				root.setAccVarsList(al);
				coveredMJoinRouters = 0;
				propagate_2(root);
			}
			root.sortAscendingProbingIndexes();
			/**Put the id for each probing sequence*/
			/**End putting the id for probing sequence*/
			if (Config.PRINT_LOG) {
				root.printLog("");
			}		
		}

		private void propagate_2(Vertex vertex) {
			//for the special case
			boolean setBefore = false;
			for (AccVars av : vertex.getAccVarsList()) {
				if (av.isSatifiedMJoinRouter() && (!av.getVisitedMJoinRouter().isCovered())) {
					if (Config.PRINT_LOG) {
						logger.info("Trivial cases have triggered into if statement");
					}
					//save the path first
					av.getVisitedMJoinRouter().saveProbingSequence(av.getLastJoinedVW(), av.getVars(), vertex.getId());
					
					av.getVisitedMJoinRouter().setCovered(true);
					
					vertex.addSatisfiedMJoinRouter(av.getVisitedMJoinRouter());
					
					coveredMJoinRouters ++;
					if (coveredMJoinRouters == mjoinRouters.size()) {
						if (Config.PRINT_LOG) {
							logger.info("Trivial cases have affected query coverage");
						}
						return;
					}
				}
			}
			/**loop until all of the queries are covered*/
			while (coveredMJoinRouters < mjoinRouters.size()) {	
				/**each step of loop, we need to manage the max coverage value call overlapped value
				it will not be accurate if the same value is created multiple times so a manager
				of these value are needed.*/
				OVManager ovm = new OVManager();
				/**with every accumulated set of variables, check it with non-visited window in the
				same query to calculate the value of the combination of shared variable position 
				in both previous set and variables in this virtual window*/
				for (AccVars av : vertex.getAccVarsList()) {
					for (VirtualWindow vTmp : wm.getVirtualWindows()) {
						//supplement: just consider all virtual windows in the same turn of registration
						//every specific consideration is just meaningful if they are in the same query
						//test
						boolean x = (av.getVisitedMJoinRouter().getId() == vTmp.getItsCurrentMJoinRouter().getId());
						boolean y = !av.visited(vTmp);
						boolean z = (!vTmp.isVisited());
						boolean w = (!vTmp.isRemoved());
						//System.out.println("x : " + x + " y: " + y + " z: " + z);
						if (x && y && z && w) {
	
							//get the shared variable(s) of these 2 sets
							ArrayList<Var> sharedVar = OpUtils.getOverlappedKey(av.getValue(), vTmp.getOp());
							
							if (sharedVar != null) {
								
								ArrayList<Integer> next = new ArrayList<Integer>();
								ArrayList<Integer> prev = new ArrayList<Integer>();
								//build the previous and the next shared variables set
								for (int i = 0; i < sharedVar.size(); i ++) {
									
									int n = OpUtils.getVarIdx(vTmp.getOp(), sharedVar.get(i));
									int p = av.getVarIdx(sharedVar.get(i));
									
									next.add(n);
									prev.add(p);
								}
								
								OverlappedValue ov = ovm.getOV(vTmp.getPW().getId(), next, prev);
								
								if (ov == null) {
									
									ov = ovm.createOV(vTmp.getPW(), next, prev);
									
								}
								ov.addTmpNextVirtualWindow(vTmp);
								
								ov.addContainedMJoinRouter(vTmp.getItsCurrentMJoinRouter());
							}
						}
					}
				}

				
				OverlappedValue ov = ovm.getMaxOV();
				
				if (ov == null)
					return;
				
				if (ov.getWeight() > 0) {
					Vertex newVertex = new Vertex(ov.getPhysicalWindow());
					newVertex.setIndexesInfo(ov.getNextVarCols());
					
					vertex.addChild(newVertex);
					
					//this new vertex contains a list of new accumulated variables set
					ArrayList<AccVars> accList = new ArrayList<AccVars>();
					newVertex.setAccVarsList(accList);
					newVertex.setArrivedEdge(vertex, ov);
					
					//consider again the pair of acc variables and virtual window
					//to find out which one is a part of composition of ov
					ArrayList<ProbingInfo> newProbingInfo = null;

					BitSet flags = new BitSet();
					for (AccVars av : vertex.getAccVarsList()) {

						for (VirtualWindow chosenV : ov.getTmpNextVirtualWindow()) {
							
							if (av.getVisitedMJoinRouter().getId() == chosenV.getItsCurrentMJoinRouter().getId()
									&& flags.get(av.getVisitedMJoinRouter().getId()) == false) {
								
								ArrayList<Var> sharedVar = OpUtils.getOverlappedKey(av.getValue(), chosenV.getOp());
								//for sure this if will be true at least 1
								if (sharedVar != null) {
									
									boolean passed = true;
									
									for (int i = 0; i < sharedVar.size(); i++) {
										
										try {
										boolean x = ov.getNextVarCol(i) != OpUtils.getVarIdx(chosenV.getOp(), sharedVar.get(i));
										boolean y = ov.getPrevVarCol(i) != av.getVarIdx(sharedVar.get(i));
										if (x || y) {
											passed = false;
											break;
										}
//										if (ov.getNextVarPos(i) != OpUtils.getVarIdx(chosenV.getOp(), sharedVar.get(i)) ||
//												ov.getPrevVarPos(i) != av.getVarIdx(sharedVar.get(i))) {
//											passed = false;
//											break;
//										}
										} catch (Exception e) {
											passed = false;
											break;
										}
									}
									if (passed) {
										
										AccVars newAcc = av.clone();
										
										newProbingInfo = newAcc.addVars(chosenV, vertex);
										
										newAcc.addVisitedVWs(chosenV);
										
										accList.add(newAcc);

										newVertex.setProbingInfo(newProbingInfo);

										//save the path
										setBefore = true;
										chosenV.setBefore(av.getLastJoinedVW());
											if (newAcc.isSatifiedMJoinRouter() && (!newAcc.getVisitedMJoinRouter().isCovered())) {
											//save the probing sequence information
											newAcc.getVisitedMJoinRouter().saveProbingSequence(chosenV, newAcc.getVars(), newVertex.getId());
											//newAcc.getVisitedMJoinRouter().saveProbingSequence(ps, newAcc.getVars(), newVertex.getId());
											
											newAcc.getVisitedMJoinRouter().setCovered(true);
											
											newVertex.addSatisfiedMJoinRouter(newAcc.getVisitedMJoinRouter());

											coveredMJoinRouters ++;
											if (coveredMJoinRouters == mjoinRouters.size())
												return;
										}
										//found++;
										flags.set(chosenV.getItsCurrentMJoinRouter().getId());
										break;										
									}
								}
							}
						}
//						if (found == ov.getWeight()) {
//							break;
//						}
						if (flags.cardinality() == ov.getWeight()) {
							break;
						}						
					}
					//ov.sortIndexPos();
					propagate_2(newVertex);
				} else  {
					return;
				}
			}
		}

}
