package uminho.dss.turmas3l.data;

import uminho.dss.turmas3l.business.Sala;
import uminho.dss.turmas3l.business.Aluno;

import java.sql.*;
import java.util.*;

import static java.util.stream.Collectors.toList;

/**
 * Versão incompleta de um DAO para Alunos
 *
 * Tabelas a criar na BD: ver método getInstance
 *
 * @author DSS
 * @version 202309151
 */
public class AlunoDAO implements Map<String, Aluno> {
    private static AlunoDAO singleton = null;

    private AlunoDAO() {
        try (Connection conn = DriverManager.getConnection(DAOconfig.URL, DAOconfig.USERNAME, DAOconfig.PASSWORD);
             Statement stm = conn.createStatement()) {
             String sql = "CREATE TABLE IF NOT EXISTS alunos (" +
                    "Num varchar(10) NOT NULL PRIMARY KEY," +
                    "Nome varchar(45) DEFAULT NULL," +
                    "Email varchar(45) DEFAULT NULL," +
                    "Turma varchar(10), foreign key(Turma) references turmas(Id))";
             stm.executeUpdate(sql);
        } catch (SQLException e) {
            // Erro a criar tabela...
            e.printStackTrace();
            throw new NullPointerException(e.getMessage());
        }
    }

    /**
     * Implementação do padrão Singleton
     *
     * @return devolve a instância única desta classe
     */
    public static AlunoDAO getInstance() {
        if (AlunoDAO.singleton == null) {
            AlunoDAO.singleton = new AlunoDAO();
        }
        return AlunoDAO.singleton;
    }

    /**
     * @return número de Alunos na base de dados
     */
    @Override
    public int size() {
        int i = 0;
        try (Connection conn = DriverManager.getConnection(DAOconfig.URL, DAOconfig.USERNAME, DAOconfig.PASSWORD);
             Statement stm = conn.createStatement();
             ResultSet rs = stm.executeQuery("SELECT count(*) FROM alunos")) {
            if (rs.next()) {
                i = rs.getInt(1);
            }
        } catch (Exception e) {
            // Erro a criar tabela...
            e.printStackTrace();
            throw new NullPointerException(e.getMessage());
        }
        return i;
    }

    /**
     * Método que verifica se existem Alunos
     *
     * @return true se existirem 0 Alunos
     */
    @Override
    public boolean isEmpty() {
        return this.size() == 0;
    }

    /**
     * Método que cerifica se um id de Aluno existe na base de dados
     *
     * @param key id da Aluno
     * @return true se a Aluno existe
     * @throws NullPointerException Em caso de erro - deveriam ser criadas exepções do projecto
     */
    @Override
    public boolean containsKey(Object key) {
        boolean r;
        try (Connection conn = DriverManager.getConnection(DAOconfig.URL, DAOconfig.USERNAME, DAOconfig.PASSWORD);
             PreparedStatement pstm = conn.prepareStatement("SELECT Num FROM alunos WHERE Num=?")) {
            pstm.setString(1, key.toString());
            try (ResultSet rs = pstm.executeQuery()) {
                r = rs.next();  // A chave existe na tabela
            }
        } catch (SQLException e) {
            // Database error!
            e.printStackTrace();
            throw new NullPointerException(e.getMessage());
        }

        return r;
    }

    @Override
    public boolean containsValue(Object value) {
        return false;
    }

    /**
     * Obter uma Aluno, dado o seu id
     *
     * @param key id da Aluno
     * @return a Aluno caso exista (null noutro caso)
     * @throws NullPointerException Em caso de erro - deveriam ser criadas exepções do projecto
     */
    @Override
    public Aluno get(Object key) {
        Aluno a = null;
        try (Connection conn = DriverManager.getConnection(DAOconfig.URL, DAOconfig.USERNAME, DAOconfig.PASSWORD);
             PreparedStatement pstm = conn.prepareStatement("SELECT * FROM alunos WHERE Num=?")) {
            pstm.setString(1, key.toString());
            try (ResultSet rs = pstm.executeQuery()) {
                if (rs.next()) {  // A chave existe na tabela
                    a = new Aluno(rs.getString("Num"),
                            rs.getString("Nome"),
                            rs.getString("Email"));
                }
            }
        } catch (SQLException e) {
            // Database error!
            e.printStackTrace();
            throw new NullPointerException(e.getMessage());
        }

        return a;
    }

    /**
     * Insere uma Aluno na base de dados
     * <p>
     * ATENÇÂO: Esta implementação é provisória.
     * Falta devolver o valor existente (caso exista um)
     * Falta remover a sala anterior, caso esteja a ser alterada
     * Deveria utilizar transacções...
     *
     * @param key o id da Aluno
     * @param t   a Aluno
     * @return para já retorna sempre null (deverá devolver o valor existente, caso exista um)
     * @throws NullPointerException Em caso de erro - deveriam ser criadas exepções do projecto
     */
    @Override
    public Aluno put(String key, Aluno a) {
        Aluno res = null;
        try (Connection conn = DriverManager.getConnection(DAOconfig.URL, DAOconfig.USERNAME, DAOconfig.PASSWORD);
             Statement stm = conn.createStatement()) {

            // Actualizar a Sala
            stm.executeUpdate(
                    "INSERT INTO alunos " +
                            "VALUES ('" + a.getNumero() + "', '" +
                            a.getNome() + "', " +
                            a.getEmail() + ") " +
                            "ON DUPLICATE KEY UPDATE Nome=Values(Nome), " +
                            "Email=Values(Email)");

            // Actualizar a Aluno
            stm.executeUpdate(
                    "INSERT INTO alunos VALUES ('" + t.getId() + "', '" + s.getNumero() + "') " +
                            "ON DUPLICATE KEY UPDATE Sala=VALUES(Sala)");

            // Actualizar os alunos da Aluno
            Collection<String> oldAl = getAlunosAluno(key, stm);
            Collection<String> newAl = t.getAlunos().stream().collect(toList());
            newAl.removeAll(oldAl);         // Alunos que entram na Aluno, em relação ao que está na BD
            oldAl.removeAll(t.getAlunos().stream().collect(toList())); // Alunos que saem na Aluno, em relação ao que está na BD
            try (PreparedStatement pstm = conn.prepareStatement("UPDATE alunos SET Aluno=? WHERE Num=?")) {
                // Remover os que saem da Aluno (colocar a NULL a coluna que diz qual a Aluno dos alunos)
                pstm.setNull(1, Types.VARCHAR);
                for (String a : oldAl) {
                    pstm.setString(2, a);
                    pstm.executeUpdate();
                }
                // Adicionar os que entram na Aluno (colocar o Id da Aluno na coluna Aluno da tabela alunos)
                // ATENÇÃO: Para já isto não vai funcionar pois os alunos não estão na tabela
                //          (não há lá nada para atualizar).  Funcionará quando tivermos um AlunoDAO
                //          a guardar os alunos na tabela 'alunos'.
                pstm.setString(1, t.getId());
                for (String a : newAl) {
                    pstm.setString(2, a);
                    pstm.executeUpdate();
                }
            }

        } catch (SQLException e) {
            // Database error!
            e.printStackTrace();
            throw new NullPointerException(e.getMessage());
        }
        return res;
    }

    @Override
    public Aluno remove(Object key) {
        return null;
    }

    @Override
    public void putAll(Map<? extends String, ? extends Aluno> m) {

    }

    @Override
    public void clear() {

    }

    @Override
    public Set<String> keySet() {
        return Set.of();
    }

    @Override
    public Collection<Aluno> values() {
        return List.of();
    }

    @Override
    public Set<Entry<String, Aluno>> entrySet() {
        return Set.of();
    }
}

