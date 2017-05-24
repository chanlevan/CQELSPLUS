package cqelsplus.execplan.oprouters;

import cqelsplus.execplan.data.IMapping;

public interface IStatefulRouter {
	public void expireOne(IMapping mu);
}
