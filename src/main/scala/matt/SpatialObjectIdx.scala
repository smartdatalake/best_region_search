package matt

class SpatialObjectIdx {

  var pos: Integer = 0;

  var dependencies: List[Integer] = List[Integer]();

  /**
   *
   * @param position
   */
  def this(position: Integer) = {
    this();
    this.pos = position;
  }

  /**
   *
   * @param newDep
   */
  def addDependency(newDep: Integer) = {
    dependencies = dependencies :+ newDep;
  }

  /**
   *
   * @return
   */
  def getSpatialObjectPosition(): Integer = {
    this.pos;
  }

  /**
   *
   * @return
   */
  def getDependencies(): List[Integer] = {
    dependencies;
  }
}