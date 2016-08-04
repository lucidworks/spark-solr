package com.lucidworks.spark.query.sql;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.*;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;

import java.util.*;

/**
 * Adapted from the Solr project
 */
public class SolrSQLSupport extends AstVisitor<Void, Integer> {

  public static Map<String,String> parseColumns(String sqlStmt) throws Exception {
    SqlParser parser = new SqlParser();
    Statement stmt = parser.createStatement(sqlStmt);
    SolrSQLSupport support = new SolrSQLSupport();
    support.process(stmt, new Integer(0));
    return support.columnAliases;
  }

  private final StringBuilder builder;
  
  protected String table;
  protected String query;
  protected List<String> fields = new ArrayList();
  protected List<SortItem> sorts;
  protected boolean isDistinct;
  protected boolean hasColumnAliases;
  protected Map<String, String> columnAliases = new HashMap();

  SolrSQLSupport() {
    this.builder = new StringBuilder();
  }

  protected Void visitNode(Node node, Integer indent) {
    throw new UnsupportedOperationException("not yet implemented: " + node);
  }
  
  protected Void visitUnnest(Unnest node, Integer indent) {
    return null;
  }

  protected Void visitQuery(Query node, Integer indent) {
    if (node.getWith().isPresent()) {
      With confidence = (With) node.getWith().get();
      this.append(indent.intValue(), "WITH");
      if (confidence.isRecursive()) {
        // TODO:
      }

      Iterator queries = confidence.getQueries().iterator();

      while (queries.hasNext()) {
        WithQuery query = (WithQuery) queries.next();
        this.process(new TableSubquery(query.getQuery()), indent);
        if (queries.hasNext()) {
          // TODO:
        }
      }
    }

    this.processRelation(node.getQueryBody(), indent);
    if (!node.getOrderBy().isEmpty()) {
      this.sorts = node.getOrderBy();
    }

    if (node.getLimit().isPresent()) {
    }

    if (node.getApproximate().isPresent()) {

    }

    return null;
  }

  protected Void visitQuerySpecification(QuerySpecification node, Integer indent) {
    this.process(node.getSelect(), indent);
    if (node.getFrom().isPresent()) {
      this.process(node.getFrom().get(), indent);
    }
    return null;
  }

  protected Void visitComparisonExpression(ComparisonExpression node, Integer index) {
    String field = node.getLeft().toString();
    String value = node.getRight().toString();
    query = stripSingleQuotes(stripQuotes(field)) + ":" + stripQuotes(value);
    return null;
  }

  protected Void visitSelect(Select node, Integer indent) {
    this.append(indent.intValue(), "SELECT");
    if (node.isDistinct()) {
      this.isDistinct = true;
    }

    if (node.getSelectItems().size() > 1) {
      boolean first = true;

      for (Iterator var4 = node.getSelectItems().iterator(); var4.hasNext(); first = false) {
        SelectItem item = (SelectItem) var4.next();
        this.process(item, indent);
      }
    } else {
      this.process((Node) Iterables.getOnlyElement(node.getSelectItems()), indent);
    }

    return null;
  }

  protected Void visitSingleColumn(SingleColumn node, Integer indent) {

    Expression ex = node.getExpression();
    String field = null;

    if (ex instanceof QualifiedNameReference) {

      QualifiedNameReference ref = (QualifiedNameReference) ex;
      List<String> parts = ref.getName().getOriginalParts();
      field = parts.get(0);

    } else if (ex instanceof FunctionCall) {

      FunctionCall functionCall = (FunctionCall) ex;
      List<String> parts = functionCall.getName().getOriginalParts();
      List<Expression> args = functionCall.getArguments();
      String col = null;

      if (args.size() > 0 && args.get(0) instanceof QualifiedNameReference) {
        QualifiedNameReference ref = (QualifiedNameReference) args.get(0);
        col = ref.getName().getOriginalParts().get(0);
        field = parts.get(0) + "(" + stripSingleQuotes(col) + ")";
      } else {
        field = stripSingleQuotes(stripQuotes(functionCall.toString()));
      }

    } else if (ex instanceof StringLiteral) {
      StringLiteral stringLiteral = (StringLiteral) ex;
      field = stripSingleQuotes(stringLiteral.toString());
    }

    fields.add(field);

    if (node.getAlias().isPresent()) {
      String alias = node.getAlias().get();
      columnAliases.put(field, alias);
      hasColumnAliases = true;
    } else {
      columnAliases.put(field, field);
    }

    return null;
  }

  private static String stripQuotes(String s) {
    StringBuilder buf = new StringBuilder();
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c != '"') {
        buf.append(c);
      }
    }
    return buf.toString();
  }


  private static String stripSingleQuotes(String s) {
    StringBuilder buf = new StringBuilder();
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c != '\'') {
        buf.append(c);
      }
    }
    return buf.toString();
  }


  protected Void visitAllColumns(AllColumns node, Integer context) {
    return null;
  }

  protected Void visitTable(Table node, Integer indent) {
    this.table = stripSingleQuotes(node.getName().toString());
    return null;
  }

  protected Void visitAliasedRelation(AliasedRelation node, Integer indent) {
    this.process(node.getRelation(), indent);
    return null;
  }

  protected Void visitValues(Values node, Integer indent) {
    boolean first = true;

    for (Iterator var4 = node.getRows().iterator(); var4.hasNext(); first = false) {
      Expression row = (Expression) var4.next();

    }

    return null;
  }

  private void processRelation(Relation relation, Integer indent) {
    if (relation instanceof Table) {
    } else {
      this.process(relation, indent);
    }
  }

  private StringBuilder append(int indent, String value) {
    return this.builder.append(indentString(indent)).append(value);
  }

  private static String indentString(int indent) {
    return Strings.repeat("   ", indent);
  }
}
