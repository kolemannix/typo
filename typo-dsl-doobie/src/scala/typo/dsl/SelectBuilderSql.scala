package typo.dsl

import cats.syntax.apply.*
import cats.syntax.foldable.*
import doobie.free.connection.ConnectionIO
import doobie.implicits.toSqlInterpolator
import doobie.util.Read
import doobie.util.fragment.Fragment
import typo.dsl.internal.mkFragment.*
import scala.util.Try

sealed trait SelectBuilderSql[Fields, Row] extends SelectBuilder[Fields, Row] {
  def withPath(path: Path): SelectBuilderSql[Fields, Row]
  def instantiate(ctx: RenderCtx): SelectBuilderSql.Instantiated[Fields, Row]

  override lazy val renderCtx: RenderCtx = RenderCtx.from(this)

  lazy val sqlAndRowParser: (Fragment, Read[Row]) = {
    val instance = this.instantiate(renderCtx)
    (instance.toQuery, instance.read)
  }

  def sqlAndRowParserProj[Row2](
      projectFields: Fields => List[SqlExpr.FieldLikeNoHkt[?, ?]],
      projectedRead: Read[Row2]
  ): (Fragment, Read[Row2]) = {
    val instance = this.instantiate(renderCtx).project[Fields, Row2](projectFields, projectedRead)
    (instance.toQuery, instance.read)
  }

  override lazy val sql: Option[Fragment] = Some(sqlAndRowParser._1)

  final override def joinOn[Fields2, N[_]: Nullability, Row2](other: SelectBuilder[Fields2, Row2])(pred: Fields ~ Fields2 => SqlExpr[Boolean, N]): SelectBuilder[Fields ~ Fields2, Row ~ Row2] =
    other match {
      case otherSql: SelectBuilderSql[Fields2, Row2] =>
        new SelectBuilderSql.TableJoin[Fields, Fields2, N, Row, Row2](this.withPath(Path.LeftInJoin), otherSql.withPath(Path.RightInJoin), pred, SelectParams.empty)
      case _ => sys.error("you cannot mix mock and sql repos")
    }

  final override def leftJoinOn[Fields2, N[_]: Nullability, Row2](
      other: SelectBuilder[Fields2, Row2]
  )(pred: Fields ~ Fields2 => SqlExpr[Boolean, N]): SelectBuilder[Fields ~ OuterJoined[Fields2], Row ~ Option[Row2]] =
    other match {
      case otherSql: SelectBuilderSql[Fields2, Row2] =>
        SelectBuilderSql.TableLeftJoin(this.withPath(Path.LeftInJoin), otherSql.withPath(Path.RightInJoin), pred, SelectParams.empty)
      case _ => sys.error("you cannot mix mock and sql repos")
    }

  final override def toList: ConnectionIO[List[Row]] = {
    val (frag, read) = sqlAndRowParser
    frag.query(using read).to[List]
  }

  final override def toListProjSingle[Field](selectField: Fields => SqlExpr.FieldLikeNoHkt[Field, Row], projectedRead: Read[Field]): ConnectionIO[List[Field]] = {
    toListWithSelect[Field](selectField.andThen(List(_)), projectedRead)
  }
  final override def toListWithSelect[Row2](
      projectFields: Fields => List[SqlExpr.FieldLikeNoHkt[?, Row]],
      projectedRead: Read[Row2]
  ): ConnectionIO[List[Row2]] = {
    val (frag, read) = sqlAndRowParserProj(projectFields, projectedRead)
    println(s"Unprojected query is:\n${sql.get}")
    println(s"Projected query is:\n${frag}")
    frag.query(using read).to[List]
  }

  override final def toListProjRow[Fields2, Row2](selectRow: Fields => Fields2): ConnectionIO[List[Row2]] = {
    val newFields = selectRow(structure.fields)
    val instance = this.instantiate(renderCtx)
    println(s"Type of newFields: ${newFields.getClass}")
    println(s"Fields of newFields: ${newFields.getClass.getFields}")
    newFields match {
      case f: SelectBuilderSql.Relation[Fields2, Row2] @unchecked =>
        val cols = f.structure.columns.map(col => (instance.alias, col))
        val newInstance = instance.copy[Fields2, Row2](columns = cols, structure = f.structure, read = f.read)
        val frag = newInstance.toQuery
        val read = f.read
        frag.query(using read).to[List]
      case _ =>
        sys.error("Match error; expected Relation")
    }
  }

  final override def count: ConnectionIO[Int] = {
    val (frag, _) = sqlAndRowParser
    fr"select count(*) from ($frag) rows".query[Int].unique
  }
}

object SelectBuilderSql {
  def apply[Fields, Row](
      name: String,
      structure: Structure.Relation[Fields, Row],
      rowParser: Read[Row]
  ): SelectBuilderSql[Fields, Row] =
    Relation(name, structure, rowParser, SelectParams.empty)

  def formatCTEs(ctes: Seq[CTE]): Seq[Fragment] = {
    ctes.map { cte =>
      fr"""|${Fragment.const0(cte.name)} as (
           |  ${cte.sql}
           |)""".stripMargin
    }
  }

  def formatColumns(cols: List[(String, SqlExpr.FieldLikeNoHkt[?, ?])]): List[String] = cols.map { case (alias, col) =>
    col.sqlReadCast.foldLeft(s"($alias).\"${col.name}\"") { case (acc, cast) => s"$acc::$cast" }
  }

  final case class Relation[Fields, Row](
      name: String,
      structure: Structure.Relation[Fields, Row],
      read: Read[Row],
      params: SelectParams[Fields, Row]
  ) extends SelectBuilderSql[Fields, Row] {
    override def withPath(path: Path): SelectBuilderSql[Fields, Row] =
      copy(structure = structure.withPath(path))

    override def withParams(sqlParams: SelectParams[Fields, Row]): SelectBuilder[Fields, Row] =
      copy(params = sqlParams)

    override def instantiate(ctx: RenderCtx): SelectBuilderSql.Instantiated[Fields, Row] = {
      val alias = ctx.alias(structure._path)
      val sql = fr"(select ${Fragment.const0(alias)} from ${Fragment.const0(name)} ${Fragment.const0(alias)} ${SelectParams.render(structure.fields, ctx, params).getOrElse(Fragment.empty)})"

      SelectBuilderSql.Instantiated(
        alias = alias,
        isJoin = false,
        columns = structure.columns.map(c => (alias, c)),
        sqlFrag = sql,
        upstreamCTEs = Nil,
        structure = structure,
        read = read
      )
    }
  }

  final case class TableJoin[Fields1, Fields2, N[_]: Nullability, Row1, Row2](
      left: SelectBuilderSql[Fields1, Row1],
      right: SelectBuilderSql[Fields2, Row2],
      pred: Fields1 ~ Fields2 => SqlExpr[Boolean, N],
      params: SelectParams[Fields1 ~ Fields2, Row1 ~ Row2]
  ) extends SelectBuilderSql[Fields1 ~ Fields2, Row1 ~ Row2] {
    override lazy val structure: Structure[(Fields1, Fields2), Row1 ~ Row2] =
      left.structure.join(right.structure)

    override def withPath(path: Path): SelectBuilderSql[Fields1 ~ Fields2, Row1 ~ Row2] =
      copy(left = left.withPath(path), right = right.withPath(path))

    override def withParams(sqlParams: SelectParams[Fields1 ~ Fields2, Row1 ~ Row2]): SelectBuilder[Fields1 ~ Fields2, Row1 ~ Row2] =
      copy(params = sqlParams)

    override def instantiate(ctx: RenderCtx): Instantiated[Fields1 ~ Fields2, Row1 ~ Row2] = {
      val alias = ctx.alias(structure._path)
      val leftInstance = left.instantiate(ctx)
      val rightInstance = right.instantiate(ctx)
      val newStructure = leftInstance.structure.join(rightInstance.structure)
      val ctes = leftInstance.asCTEs ++ rightInstance.asCTEs
      val sql =
        fr"""|select ${ctes.filterNot(_.isJoin).map(cte => Fragment.const0(cte.name)).mkFragment(Fragment.const0(", "))}
               |  from ${Fragment.const0(leftInstance.alias)}
               |  join ${Fragment.const0(rightInstance.alias)}
               |  on ${pred(newStructure.fields).render(ctx)}
               |  ${SelectParams.render(newStructure.fields, ctx, params).getOrElse(Fragment.empty)}""".stripMargin
      SelectBuilderSql.Instantiated(
        alias = alias,
        isJoin = true,
        columns = leftInstance.columns ++ rightInstance.columns,
        sqlFrag = sql,
        upstreamCTEs = ctes,
        structure = newStructure,
        read = (leftInstance.read, rightInstance.read).tupled
      )
    }
  }

  final case class TableLeftJoin[Fields1, Fields2, N[_]: Nullability, Row1, Row2](
      left: SelectBuilderSql[Fields1, Row1],
      right: SelectBuilderSql[Fields2, Row2],
      pred: Fields1 ~ Fields2 => SqlExpr[Boolean, N],
      params: SelectParams[Fields1 ~ OuterJoined[Fields2], Row1 ~ Option[Row2]]
  ) extends SelectBuilderSql[Fields1 ~ OuterJoined[Fields2], Row1 ~ Option[Row2]] {
    override lazy val structure: Structure[Fields1 ~ OuterJoined[Fields2], Row1 ~ Option[Row2]] =
      left.structure.leftJoin(right.structure)

    override def withPath(path: Path): SelectBuilderSql[Fields1 ~ OuterJoined[Fields2], Row1 ~ Option[Row2]] =
      copy(left = left.withPath(path), right = right.withPath(path))

    override def withParams(sqlParams: SelectParams[Fields1 ~ OuterJoined[Fields2], Row1 ~ Option[Row2]]): SelectBuilder[Fields1 ~ OuterJoined[Fields2], Row1 ~ Option[Row2]] =
      copy(params = sqlParams)

    def opt[A](read: Read[A]): Read[Option[A]] = {
      new Read[Option[A]](
        read.gets.map { case (get, _) => (get, doobie.enumerated.Nullability.Nullable) },
        (rs, i) => Try(read.unsafeGet(rs, i)).toOption
      )
    }

    override def instantiate(ctx: RenderCtx): Instantiated[Fields1 ~ OuterJoined[Fields2], Row1 ~ Option[Row2]] = {
      val alias = ctx.alias(structure._path)
      val leftInstance = left.instantiate(ctx)
      val rightInstance = right.instantiate(ctx)

      val joinedStructure = leftInstance.structure.join(rightInstance.structure)
      val newStructure = leftInstance.structure.leftJoin(rightInstance.structure)
      val ctes = leftInstance.asCTEs ++ rightInstance.asCTEs
      val sql =
        fr"""|select ${ctes.filterNot(_.isJoin).map(cte => Fragment.const0(cte.name)).mkFragment(Fragment.const0(", "))}
               |  from ${Fragment.const0(leftInstance.alias)}
               |  left join ${Fragment.const0(rightInstance.alias)}
               |  on ${pred(joinedStructure.fields).render(ctx)}
               |  ${SelectParams.render(newStructure.fields, ctx, params).getOrElse(Fragment.empty)}""".stripMargin

      SelectBuilderSql.Instantiated(
        alias = alias,
        isJoin = true,
        columns = leftInstance.columns ++ rightInstance.columns,
        sqlFrag = sql,
        upstreamCTEs = ctes,
        structure = newStructure,
        read = (leftInstance.read, opt(rightInstance.read)).tupled
      )
    }
  }

  /** Need this intermediate data structure to generate aliases for tables (and prefixes for column selections) when we have a tree of joined tables. Need to start from the root after the user has
    * constructed the tree
    */
  final case class Instantiated[Fields, Row](
      alias: String,
      isJoin: Boolean,
      columns: List[(String, SqlExpr.FieldLikeNoHkt[?, ?])],
      sqlFrag: Fragment,
      upstreamCTEs: List[CTE],
      structure: Structure[Fields, Row],
      read: Read[Row]
  ) {
    def asCTEs: List[CTE] = upstreamCTEs :+ CTE(alias, sqlFrag, isJoin)

    def toQuery: Fragment = {
      val cols = formatColumns(columns)
      val ctes = this.asCTEs
      val formattedCTEs = formatCTEs(ctes)

      val frag =
        fr"""|with
             |${formattedCTEs.mkFragment(Fragment.const0(",\n"))}
             |select ${Fragment.const0(cols.mkString(","))} from ${Fragment.const0(ctes.last.name)}""".stripMargin

      frag
    }

    def project[Fields2, Row2](
        projectFields: Fields => List[SqlExpr.FieldLikeNoHkt[?, ?]],
        projectedRead: Read[Row2]
    ): Instantiated[Fields2, Row2] = {
      Instantiated[Fields2, Row2](
        alias,
        isJoin,
        projectFields(structure.fields).map(f => (alias, f)),
        sqlFrag,
        upstreamCTEs,
        structure.asInstanceOf[Structure[Fields2, Row2]],
        projectedRead
      )
    }
  }
  case class CTE(name: String, sql: Fragment, isJoin: Boolean)

//  final case class Projector[Fields, Row](
//      instance: Instantiated[Fields, Row],
//  ) {
//    def toList
//  }
}
