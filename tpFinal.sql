--base de datos dbTemporal (es la bdd de contiene las tablas del tp, y el sp Compare)
USE master
GO
IF EXISTS(select * from sys.databases where name= 'DB_Temporal')
begin
ALTER DATABASE DB_Temporal SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
EXEC('DROP DATABASE ' + 'DB_Temporal')
end

create database DB_Temporal
go
use DB_Temporal
go

create table BaseDeDatos(
	BaseDeDatosId int identity(1,1) not null,
	Nombre varchar(100),
	Funcion varchar(10),
	constraint PK_BaseDeDatos primary key (BaseDeDatosId)
)
go
create table Tabla_Crear(
	Tabla_CrearId int not null,
	Esquema varchar(100),
	Nombre varchar(100),
	constraint PK_Tabla_Crear primary key (Tabla_CrearId),
	
)
go
create table Tabla_Eliminar(
	Tabla_EliminarId int not null,
	Esquema varchar(100),
	Nombre varchar(100),
	constraint PK_Tabla_Eliminar primary key (Tabla_EliminarId),
	
)
go
create table Tabla_Modificar(
	Tabla_ModificarId int not null,
	Esquema varchar(100),
	Nombre varchar(100),
	constraint PK_Tabla_Modificar primary key (Tabla_ModificarId),
	
)
go
create table Columna(
	ColumnaId int identity(1,1) not null,
	Nombre varchar(100),
	TipoDeDato varchar(50),
	Longitud varchar(10),
	EsNuleable varchar(5),
	EsIdentity bit,
	ColumnaDefault varchar(8000),
	Tabla_CrearId int,
	Tabla_ModificarId int,
	EsAddColumn bit,
	EsDropColumn bit,
	EsAlterColumn bit,
	Referencia varchar(50),
	NombreTablaReferencia varchar(50),
	Pk varchar(20),
	constraint PK_Columna primary key (ColumnaId),
	constraint FK_Columna_Tabla_Crear foreign key (Tabla_CrearId) 
		references Tabla_Crear(Tabla_CrearId),
	constraint FK_Columna_Tabla_Modificar foreign key (Tabla_ModificarId) 
	references Tabla_Modificar(Tabla_ModificarId)
)
go

create table ClaveForaneaCrear(
	ClaveForaneaCrearId int identity(1,1) not null
	constraint PK_ClaveForaneaCrear primary key(ClaveForaneaCrearId),
	Esquema varchar(20),
	TablaConFk varchar(50),
	NombreFK varchar(100),
	NombreColumnaFk varchar(50),
	TablaReferences varchar (50),
	NombreColumnaReferencia varchar(50)
)
go

create table OtrasConstraintCrear(
	OtrasConstraintCrearId int identity(1,1) not null,
	constraint PK_OtrasConstraintCrear primary key(OtrasConstraintCrearId),
	Esquema varchar(20),
	NombreTabla varchar(50),
	NombreConstraint varchar(100),
	TipoConstraint varchar(20),
	ColumnaConConstraint varchar(100),
	Definicion varchar(100)
)
go

create table ConstraintEliminar(
	ConstraintEliminarId int identity(1,1) not null,
	constraint PK_ConstraintEliminar primary key(ConstraintEliminarId),
	Esquema varchar(20),
	NombreTabla varchar(50),
	NombreConstraint varchar(100)
)
go


create table LogErrores(
	LogErroresId int identity(1,1) not null
	constraint PK_LogErrores primary key (LogErroresId),
	NombreProcedimiento varchar(100),
	MensajeError varchar(max),
	Linea int
)
go

create table Norma(
	NormaId int identity(1,1),
	constraint PK_Norma primary key (NormaId),
	Origen varchar(50),
	Nombre varchar(100),
	Motivo varchar(max)
)
go

/*creamos 2 bases de datos (Origen y Destino para hacer pruebas)*/
use Master 
go
if exists(select * from sys.databases where name = 'Origen')
begin
drop database Origen
end
if exists(select * from sys.databases where name = 'Destino')
begin
drop database Destino
end
create database Destino
go
use Destino
create table Tabla1(
	Id int not null,
	Telefono nvarchar(100),
	CampoExtra varchar(50),
	CampoCambiado varchar(50),
	DefaultMismoNombre datetime
	constraint PK_Tabla1 primary key (Id)
)
go

create table Tabla2(
	Id int not null,
	Edad int
	constraint PK_Tabla2 primary key (Id)
)
go

create table Tabla3(
	Id int identity(1,1) not null,
	Telefono varchar(50),
	Tabla1Id int
	constraint PK_Tabla3 primary key (Id),
	constraint FK_Tabla3_Tabla1 foreign key (Tabla1Id) references Tabla1 (Id)
)
go


create database Origen
go
use Origen

create table Tabla1(
	Id int identity(1,1) not null,
	constraint PK_Tabla1 primary key (Id),
	Nombre varchar(50),
	constraint UQ_Nombre Unique(Nombre),
	CampoCambiado varchar(100),
	DefaultMismoNombre datetime default (getdate()),
	NumeroConCheck int
	constraint CH_NumeroConCheck Check(NumeroConCheck>10)
)
go

create table Tabla2(
	Id int not null,
	constraint PK_Tabla2 primary key (Id),
	Apellido varchar(50),
	IdTabla1 int
	constraint FK_Tabla2_Tabla1 foreign key (IdTabla1) references Tabla1 (Id)
)
go

--no hay tabla3
create table Tabla4(
	Id int identity(1,1) not null,
	Sexo varchar(1),
	FechaNacimiento datetime
	constraint PK_Tabla4 primary key (Id)
)
go

create table Tabla5(
	Id int not null,
	OtraFecha datetime default(getdate())
)
go

create table Tabla6(
	Id int null,
	Campo bit
)
go

/*Procedimiento para crear archivos que se agrega a la bdd dbTemporal*/
use DB_Temporal
go
IF EXISTS (SELECT * FROM sys.procedures WHERE object_id = OBJECT_ID(N'sp_WriteStringToFile'))
drop proc sp_WriteStringToFile
go
create PROCEDURE sp_WriteStringToFile
 (
@String Varchar(max),
@Path VARCHAR(255),
@Filename VARCHAR(100)

)
AS
DECLARE  @objFileSystem int
        ,@objTextStream int,
		@objErrorObject int,
		@strErrorMessage Varchar(1000),
	    @Command varchar(1000),
	    @hr int,
		@fileAndPath varchar(80)

set nocount on

select @strErrorMessage='opening the File System Object'
EXECUTE @hr = sp_OACreate  'Scripting.FileSystemObject' , @objFileSystem OUT

Select @FileAndPath=@path+'\'+@filename
if @HR=0 Select @objErrorObject=@objFileSystem , @strErrorMessage='Creating file "'+@FileAndPath+'"'
if @HR=0 execute @hr = sp_OAMethod   @objFileSystem   , 'CreateTextFile'
	, @objTextStream OUT, @FileAndPath,2,True

if @HR=0 Select @objErrorObject=@objTextStream, 
	@strErrorMessage='writing to the file "'+@FileAndPath+'"'
if @HR=0 execute @hr = sp_OAMethod  @objTextStream, 'Write', Null, @String

if @HR=0 Select @objErrorObject=@objTextStream, @strErrorMessage='closing the file "'+@FileAndPath+'"'
if @HR=0 execute @hr = sp_OAMethod  @objTextStream, 'Close'

if @hr<>0
	begin
	Declare 
		@Source varchar(255),
		@Description Varchar(255),
		@Helpfile Varchar(255),
		@HelpID int
	
	EXECUTE sp_OAGetErrorInfo  @objErrorObject, 
		@source output,@Description output,@Helpfile output,@HelpID output
	Select @strErrorMessage='Error whilst '
			+coalesce(@strErrorMessage,'doing something')
			+', '+coalesce(@Description,'')
	raiserror (@strErrorMessage,16,1)
	end
EXECUTE  sp_OADestroy @objTextStream
EXECUTE sp_OADestroy @objFileSystem
go

--esto es para permitir ole automation
sp_configure 'show advanced options', 1
GO
RECONFIGURE
GO
sp_configure 'Ole Automation Procedures', 1
GO
RECONFIGURE
GO


/*****Funciones para creacion de script **********/
IF EXISTS (SELECT * FROM   sys.objects where type = 'FN' and name = 'EliminarTabla')
drop function EliminarTabla
go
create function EliminarTabla() returns nvarchar(max)
as
begin
	declare @esquema varchar(20), @nombreTabla varchar(20)
	declare @queryRetorno nvarchar(max)
	DECLARE cursorTablaEliminar CURSOR
	for
	select tc.Esquema, tc.Nombre from Tabla_Eliminar tc 
	OPEN cursorTablaEliminar
	FETCH NEXT FROM cursorTablaEliminar into @esquema, @nombreTabla
	set @queryRetorno = ''
	WHILE @@FETCH_STATUS = 0
	begin
		set @queryRetorno += 'DROP TABLE ' +@esquema +'.'+@nombreTabla + char(13) + 'GO' + char(13)
	
		FETCH NEXT FROM cursorTablaEliminar into @esquema, @nombreTabla
	end
	CLOSE cursorTablaEliminar
	DEALLOCATE cursorTablaEliminar  
	return @queryRetorno
end
go

IF EXISTS (SELECT * FROM   sys.objects where type = 'FN' and name = 'CrearTabla')
drop function CrearTabla
go
create function CrearTabla() returns nvarchar(max)
as
begin
	declare @esquema varchar(20), @nombreTabla varchar(20), @nombreColumna varchar(20), 
	@tipoDeDato varchar(20), @Longitud varchar(20), @esNuleable varchar(5), @esIdentity bit, 
	@columnaDefault varchar(20), @tablaSiguiente varchar(20), @tablaActual varchar(20)
	declare @queryRetorno nvarchar(max)
	declare @nombreBaseDeDatos varchar(50) --seria el nombre del destino
	select @nombreBaseDeDatos = Nombre from BaseDeDatos where Funcion = 'destino'
	set @queryRetorno = 'USE ' + @nombreBaseDeDatos + char(13) + 'GO' + char(13)


	DECLARE cursorTablaCrear CURSOR
	for
	select tc.Esquema, tc.Nombre, c.Nombre, c.TipoDeDato, c.Longitud,
	c.EsNuleable, c.EsIdentity, c.ColumnaDefault 
	from Tabla_Crear tc join Columna c 
	on tc.Tabla_CrearId = c.Tabla_CrearId

	OPEN cursorTablaCrear
	FETCH NEXT FROM cursorTablaCrear into @esquema, @nombreTabla, @nombreColumna, @tipoDeDato, @Longitud,
	@esNuleable, @esIdentity, @columnaDefault
	set @queryRetorno += 'CREATE TABLE '+@esquema+'.'+@nombreTabla+'('+ char(13)
	set @tablaActual = @nombreTabla
	WHILE @@FETCH_STATUS = 0
	begin
		set @queryRetorno += @nombreColumna +' '+@tipoDeDato + IIF(@esIdentity = 1 , ' identity(1,1) ', '')
		+ @Longitud + ' ' + IIF(@esNuleable = 'YES', '', 'not null')
		+ IIF(@columnaDefault is null, '', 'DEFAULT' + @columnaDefault)
	
		FETCH NEXT FROM cursorTablaCrear into @esquema, @nombreTabla, @nombreColumna, @tipoDeDato, @Longitud,
		@esNuleable, @esIdentity, @columnaDefault
		set @tablaSiguiente = @nombreTabla
		if @tablaSiguiente != @tablaActual
			begin
				set @queryRetorno +=')'+char(13)+'GO'+char(13)
				set @tablaActual = @nombreTabla
				set @queryRetorno += 'CREATE TABLE '+@esquema+'.'+@nombreTabla+'('+ char(13)
			end
		else
		
			if @@fetch_status = 0
			SET @queryRetorno += ',' + char(13)		
	
			else
			SET @queryRetorno += CHAR(13)
	end
	set @queryRetorno +=')'+char(13)+'GO'+char(13)
	CLOSE cursorTablaCrear
	DEALLOCATE cursorTablaCrear  
	return @queryRetorno
end
go

IF EXISTS (SELECT * FROM   sys.objects where type = 'FN' and name = 'ModificarTablaAddColumn')
drop function ModificarTablaAddColumn
go
create function ModificarTablaAddColumn() returns nvarchar(max)
as
begin
	declare @esquema varchar(20), @nombreTabla varchar(20), @nombreColumna varchar(20), 
	@tipoDeDato varchar(20), @Longitud varchar(20), @esNuleable varchar(5), @esIdentity bit, 
	@columnaDefault varchar(20), @queryRetorno nvarchar(max)

	DECLARE cursorAddColumn CURSOR
	for
	select tm.Esquema, tm.Nombre, c.Nombre, c.TipoDeDato, c.Longitud,
	c.EsNuleable, c.EsIdentity, c.ColumnaDefault 
	from Tabla_Modificar tm join Columna c on tm.Tabla_ModificarId = c.Tabla_ModificarId
	where EsAddColumn = 1

	OPEN cursorAddColumn
	FETCH NEXT FROM cursorAddColumn into @esquema, @nombreTabla, @nombreColumna, @tipoDeDato, @Longitud,
	@esNuleable, @esIdentity, @columnaDefault
	set @queryRetorno = ''
	WHILE @@FETCH_STATUS = 0
	begin
	
		set @queryRetorno += 'ALTER TABLE '+@esquema+'.'+@nombreTabla + ' ADD '
		+ @nombreColumna +' '+@tipoDeDato + IIF(@esIdentity = 1 , ' identity(1,1) ', '')
		+ @Longitud + ' ' + IIF(@esNuleable = 'YES', '', 'not null')
		+ IIF(@columnaDefault is null, '', 'DEFAULT' + @columnaDefault) + char(13) + 'GO' + char(13)
	
		FETCH NEXT FROM cursorAddColumn into @esquema, @nombreTabla, @nombreColumna, @tipoDeDato, @Longitud,
		@esNuleable, @esIdentity, @columnaDefault
	end
	CLOSE cursorAddColumn
	DEALLOCATE cursorAddColumn  
	return @queryRetorno
end
go

IF EXISTS (SELECT * FROM   sys.objects where type = 'FN' and name = 'ModificarTablaDropColumn')
drop function ModificarTablaDropColumn
go
create function ModificarTablaDropColumn() returns nvarchar(max)
as
begin
	declare @esquema varchar(20), @nombreTabla varchar(20), @nombreColumna varchar(20)
	,@queryRetorno nvarchar(max)

	DECLARE cursorDropColumn CURSOR
	for
	select tm.Esquema, tm.Nombre, c.Nombre
	from Tabla_Modificar tm join Columna c on tm.Tabla_ModificarId = c.Tabla_ModificarId
	where EsDropColumn = 1

	OPEN cursorDropColumn
	FETCH NEXT FROM cursorDropColumn into @esquema, @nombreTabla, @nombreColumna
	set @queryRetorno = ''
	WHILE @@FETCH_STATUS = 0
	begin
	
		set @queryRetorno += 'ALTER TABLE '+@esquema+'.'+@nombreTabla + ' DROP COLUMN '
		+ @nombreColumna + char(13) + 'GO' + char(13)
	
		FETCH NEXT FROM cursorDropColumn into @esquema, @nombreTabla, @nombreColumna
	
	end

	CLOSE cursorDropColumn
	DEALLOCATE cursorDropColumn  
	return @queryRetorno
end
go

IF EXISTS (SELECT * FROM   sys.objects where type = 'FN' and name = 'ModificarTablaAlterColumn')
drop function ModificarTablaAlterColumn
go
create function ModificarTablaAlterColumn() returns nvarchar(max)
as
begin
	declare @esquema varchar(20), @nombreTabla varchar(20), @nombreColumna varchar(20), 
	@tipoDeDato varchar(20), @Longitud varchar(20), @esNuleable varchar(5), @esIdentity bit, 
	@columnaDefault varchar(20), @referencia varchar(50), @nombreTablaReferencia varchar(50),
	@pk varchar(30), @queryRetorno nvarchar(max)

	DECLARE cursorAlterColumn CURSOR
	for
	select tm.Esquema, tm.Nombre, c.Nombre, c.TipoDeDato, c.Longitud,
	c.EsNuleable, c.EsIdentity, c.ColumnaDefault, c.Referencia, c.NombreTablaReferencia, c.Pk 
	from Tabla_Modificar tm join Columna c on tm.Tabla_ModificarId = c.Tabla_ModificarId
	where EsAlterColumn = 1

	OPEN cursorAlterColumn
	FETCH NEXT FROM cursorAlterColumn into @esquema, @nombreTabla, @nombreColumna, @tipoDeDato, @Longitud,
	@esNuleable, @esIdentity, @columnaDefault, @referencia, @nombreTablaReferencia, @pk
	set @queryRetorno = ''
	WHILE @@FETCH_STATUS = 0
	begin
		IF(@esIdentity = 1)
			begin
				set @queryRetorno += 'Alter Table '+@esquema+'.'+@nombreTabla+' ADD Id_new Int Identity(1,1) '
				+IIF(@esNuleable = 'YES', '', 'not null')+char(13)+'Go'+char(13)+
				'Alter Table '+@esquema+'.'+@nombreTablaReferencia+' Drop constraint '+ @referencia+char(13)+
				'Alter Table '+@esquema+'.'+@nombreTabla+' Drop constraint '+@pk+char(13)+
				'Alter Table '+@esquema+'.'+@nombreTabla+' Drop Column '+@nombreColumna+char(13)+'Go'+char(13)+
				'Exec sp_rename '+''''+@esquema+'.'+@nombreTabla+'.Id_new'', '''+@nombreColumna+''',''Column'''+char(13)
				+ 'Alter table '+@esquema+'.'+@nombreTabla+' ADD constraint PK_'+@nombreTabla+
				' primary key ('+@nombreColumna+')'+char(13)
				
			end
		ELSE
		set @queryRetorno += 'ALTER TABLE '+@esquema+'.'+@nombreTabla + ' ALTER COLUMN '
		+ @nombreColumna +' '+@tipoDeDato 
		--+ IIF(@esIdentity = 1 , ' identity(1,1) ', '')
		+ @Longitud + ' ' + IIF(@esNuleable = 'YES', '', 'not null')
		+ IIF(@columnaDefault is null, '', 
			char(13) + 'ALTER TABLE '+@esquema+'.'+@nombreTabla +' ADD DEFAULT ' + @columnaDefault +
			' for '+ @nombreColumna ) 
			+ char(13) + 'GO' + char(13)
	
		FETCH NEXT FROM cursorAlterColumn into @esquema, @nombreTabla, @nombreColumna, @tipoDeDato, @Longitud,
		@esNuleable, @esIdentity, @columnaDefault, @referencia, @nombreTablaReferencia, @pk
	end
	CLOSE cursorAlterColumn
	DEALLOCATE cursorAlterColumn  
	return @queryRetorno
end
go

IF EXISTS (SELECT * FROM   sys.objects where type = 'FN' and name = 'AgregarFk')
drop function AgregarFk
go
create function AgregarFk() returns nvarchar(max)
as
begin
	declare @esquema varchar(20), @nombreTabla varchar(20), @nombreFk varchar(50), 
	@nombreColumnaFk varchar(50), @tablaReferences varchar(50), @nombreColumnaReferencia varchar(50),
	@queryRetorno nvarchar(max)

	DECLARE cursorAgregarFk CURSOR
	for
	select fk.Esquema, fk.TablaConFk, fk.NombreFK, fk.NombreColumnaFk, 
	fk.TablaReferences, fk.NombreColumnaReferencia
	from ClaveForaneaCrear fk 

	OPEN cursorAgregarFk
	FETCH NEXT FROM cursorAgregarFk into @esquema, @nombreTabla, @nombreFk, @nombreColumnaFk, 
	@tablaReferences, @nombreColumnaReferencia
	set @queryRetorno = ''
	WHILE @@FETCH_STATUS = 0
	begin
		set @queryRetorno += 'IF NOT EXISTS (SELECT * FROM dbo.sysobjects WHERE id = object_id' +'('''+@nombreFk+''')'+')'+
			char(13)
		set @queryRetorno += 'ALTER TABLE '+@esquema+'.'+@nombreTabla + ' ADD CONSTRAINT '
		+ @nombreFk +' FOREIGN KEY '+ '('+@nombreColumnaFk+')' + ' REFERENCES ' + @tablaReferences +
		'('+@nombreColumnaReferencia +')'+char(13)+'GO'+char(13)
	
		FETCH NEXT FROM cursorAgregarFk into @esquema, @nombreTabla, @nombreFk, @nombreColumnaFk, 
		@tablaReferences, @nombreColumnaReferencia
	end
	CLOSE cursorAgregarFk
	DEALLOCATE cursorAgregarFk  
	return @queryRetorno
end
go

IF EXISTS (SELECT * FROM   sys.objects where type = 'FN' and name = 'AgregarOtrasConstraint')
drop function AgregarOtrasConstraint
go
create function AgregarOtrasConstraint() returns nvarchar(max)
as
begin
	declare @esquema varchar(20), @nombreTabla varchar(20), @nombreConstraint varchar(50), 
	@tipoConstraint varchar(50), @columnaConConstraint varchar(50), @definicion varchar(100), 
	@queryRetorno nvarchar(max)

	DECLARE cursorAgregarOtrasConstraint CURSOR
	for
	select ot.Esquema, ot.NombreTabla, ot.NombreConstraint, ot.TipoConstraint, 
	ot.ColumnaConConstraint, ot.Definicion
	from OtrasConstraintCrear ot
	OPEN cursorAgregarOtrasConstraint
	FETCH NEXT FROM cursorAgregarOtrasConstraint into @esquema, @nombreTabla ,@nombreConstraint, 
	@tipoConstraint, @columnaConConstraint, @definicion
	set @queryRetorno = ''
	WHILE @@FETCH_STATUS = 0
	begin
	
		set @queryRetorno += 'IF NOT EXISTS (SELECT * FROM dbo.sysobjects WHERE id = object_id' +'('''+@nombreConstraint+''')'+')'+
			char(13)	
		set @queryRetorno += 'ALTER TABLE '+@esquema+'.'+@nombreTabla + ' ADD CONSTRAINT '
		+ @nombreConstraint +' '+@tipoConstraint+' '+ 
		IIF(@definicion is not null, 
		'('+@definicion+')',
		'('+@columnaConConstraint+')')
		+char(13)+'GO'+char(13)
	
		FETCH NEXT FROM cursorAgregarOtrasConstraint into @esquema, @nombreTabla ,@nombreConstraint, 
		@tipoConstraint, @columnaConConstraint, @definicion
	end
	CLOSE cursorAgregarOtrasConstraint
	DEALLOCATE cursorAgregarOtrasConstraint  
	return @queryRetorno
end
go

IF EXISTS (SELECT * FROM   sys.objects where type = 'FN' and name = 'EliminarConstraint')
drop function EliminarConstraint
go
create function EliminarConstraint() returns nvarchar(max)
as
begin
	declare @esquema varchar(20), @nombreTabla varchar(20), @nombreConstraint varchar(50), 
	@tipoConstraint varchar(50), @columnaConConstraint varchar(50), @definicion varchar(100), 
	@queryRetorno nvarchar(max)

	DECLARE cursorEliminarConstraint CURSOR
	for
	select ce.Esquema, ce.NombreTabla, ce.NombreConstraint
	from ConstraintEliminar ce
	OPEN cursorEliminarConstraint
	FETCH NEXT FROM cursorEliminarConstraint into @esquema, @nombreTabla ,@nombreConstraint
	set @queryRetorno = ''
	WHILE @@FETCH_STATUS = 0
	begin
		
		set @queryRetorno += 'IF EXISTS (SELECT * FROM dbo.sysobjects WHERE id = object_id' +'('''+@nombreConstraint+''')'+')'+
			char(13)
		set @queryRetorno += 'ALTER TABLE '+@esquema+'.'+@nombreTabla + ' DROP CONSTRAINT '
		+ @nombreConstraint +char(13)+'GO'+char(13)
	
		FETCH NEXT FROM cursorEliminarConstraint into @esquema, @nombreTabla ,@nombreConstraint
	end
	CLOSE cursorEliminarConstraint
	DEALLOCATE cursorEliminarConstraint  
	return @queryRetorno
end
go



------procedimiento final--------
-- ==================================================================================================
-- Autores: Balanda, Sergio
--			Casuscelli, Alejandra
--			Lucero, Nicolás
--			falta una
--			Martin, Florencia	
-- Fecha de Creación: 05/11/2018
-- Descripción:	Stored procedure que realiza una comparación entre 2 bases de datos (una de origen
-- y otra destino) y devuelve un archivo .sql que recreará la estructura de la base de datos de origen
-- en la base de datos de destino
-- ==================================================================================================

IF  EXISTS (SELECT * FROM sys.procedures WHERE object_id = OBJECT_ID(N'sp_Compare'))
drop proc sp_Compare
go
create proc sp_Compare(@nombre_db_Origen varchar(100), @nombre_db_Destino varchar(100))
as
begin
	begin try
		set nocount on
		--limpiamos los datos de las tablas
		delete BaseDeDatos
		delete Columna
		delete Tabla_Crear
		delete Tabla_Eliminar
		delete Tabla_Modificar
		delete ClaveForaneaCrear
		delete OtrasConstraintCrear
		delete ConstraintEliminar
		begin tran

			declare @mensajeOrigen nvarchar(120)
			set @mensajeOrigen = 'No existe la Base de Datos ' + @nombre_db_Origen
			declare @mensajeDestino nvarchar(120)
			set @mensajeDestino = 'No existe la Base de Datos ' + @nombre_db_Destino
			--verifico si existen las bdd
			if((select 1 from sys.databases where name = @nombre_db_Origen) is null)
				throw 51000, @mensajeOrigen , 1; 
			if((select 1 from sys.databases where name = @nombre_db_Destino) is null)
				throw 51000, @mensajeDestino, 1;
			
			declare @sqlTabla nvarchar(max)
			declare @sqlColumna nvarchar(max)
			declare @sqlForanea nvarchar(max)
			declare @sqlOtrasConstraint nvarchar(max)

			--agrego las bdd que llegaron por parametro en nuestra tabla de bdd
			insert into BaseDeDatos(Nombre,Funcion)
			values(@nombre_db_Origen, 'origen'), (@nombre_db_Destino, 'destino')

			--devuelve tablas que estan en Origen y no en destino, por lo tanto hay
			--que crearlas y se insertaran en Tabla_Crear
			set @sqlTabla = N'select t.object_id as Id, i.table_schema, t.name as Nombre
			from ' + @nombre_db_Origen +'.sys.tables t join '
			+@nombre_db_Origen+ '.INFORMATION_SCHEMA.TABLES i on t.name = i.TABLE_NAME
			where t.name not in (select tt.name from '+ @nombre_db_Destino +'.sys.tables tt)'
	
			insert into Tabla_Crear
			exec sp_executesql @sqlTabla

			--esta sentencia es para obtener las columnas que estan en las tablas que se van
			-- a insertar
			set @sqlColumna= N'Use '+@nombre_db_Origen+char(13)+'select i.column_name as Nombre, i.DATA_TYPE as TipoDeDato ,
			IIF (i.CHARACTER_MAXIMUM_LENGTH is not null,
				concat(''('',CONVERT(varchar(10), i.CHARACTER_MAXIMUM_LENGTH),'')''),'''') as Longitud,
			 i.is_nullable as EsNuleable, 
			 columnproperty(t.object_id, i.COLUMN_NAME, ''IsIdentity'') as EsIdentity,
			 i.column_default as ColumnaDefault, t.object_id as TablaCrearId, null as TablaModificarId,
			 0 as EsAddColumn, 0 as EsDropColumn, 0 as EsAlterColumn, null, null, null from ' 
				+@nombre_db_Origen+'.sys.tables t JOIN '+@nombre_db_Origen+'.INFORMATION_SCHEMA.COLUMNS i
					ON t.name = i.table_name
					where t.name not in (select tt.name from '+ @nombre_db_Destino+'.sys.tables tt)
					order by t.object_id'
			
			insert into Columna
			exec sp_executesql @sqlColumna

			--sentencia para ver que tablas estan en destino y no en origen. El resultado lo
			--grabo en la tabla de tablas a eliminar.
			set @sqlTabla = N'select t.object_id as Id, i.table_schema, t.name as Nombre
			from ' + @nombre_db_Destino +'.sys.tables t join '
			+@nombre_db_Destino+ '.INFORMATION_SCHEMA.TABLES i on t.name = i.TABLE_NAME
			where t.name not in (select tt.name from '+ @nombre_db_Origen +'.sys.tables tt)'

			insert into Tabla_Eliminar
			exec sp_executesql @sqlTabla

			--busco tablas con el mismo nombre, seran las tablas en la que se modifican los campos
			set @sqlTabla = 'select d.object_id, i.TABLE_SCHEMA, origen.name from '+@nombre_db_Origen+'.sys.tables 
			origen join '+@nombre_db_Origen+'.INFORMATION_SCHEMA.TABLES i
			on origen.name = i.TABLE_NAME join '+@nombre_db_Destino+'.sys.tables d on origen.name = d.name
			where origen.name in (select destino.name from '+@nombre_db_Destino+'.sys.tables destino)'

			insert into Tabla_Modificar
			exec sp_executesql @sqlTabla

			--busco por tablas del mismo nombre en origen y en destino y me fijo que campos
			--estan en origen y no en destino por lo que deberan insertarse en tabla columnas
			--con EsAddColumn en 1 y tablaModificarId referenciando a la tabla
			set @sqlColumna = 'SELECT DISTINCT o.column_name as Nombre, o.DATA_TYPE as TipoDeDato ,
			IIF (o.CHARACTER_MAXIMUM_LENGTH is not null,
			concat(''('',CONVERT(varchar(10), o.CHARACTER_MAXIMUM_LENGTH),'')''),'''') as Longitud,
			 o.is_nullable as EsNuleable, null as EsIdentity,
			 o.column_default as ColumnaDefault, null as TablaCrearId, t.object_id as TablaModificarId,
			 1 as EsAddColumn, 0 as EsDropColumn, 0 as EsAlterColumn, null, null, null    
			FROM '+@nombre_db_Origen+'.INFORMATION_SCHEMA.COLUMNS o join '+@nombre_db_Destino+'.INFORMATION_SCHEMA.COLUMNS d
			on o.TABLE_NAME = d.TABLE_NAME join '+@nombre_db_Destino+'.sys.tables t on o.TABLE_NAME = t.name
			WHERE o.COLUMN_NAME NOT IN(
				SELECT DISTINCT COLUMN_NAME
				FROM '+@nombre_db_Destino+'.INFORMATION_SCHEMA.COLUMNS)
			order by t.object_id'

			insert into Columna
			exec sp_executesql @sqlColumna

			--busco por tablas del mismo nombre pero que las columnas en destino no esten en origen
			--o sea, que se debe hacer un dropColumn (se pone en 1 EsDropColumn)
			set @sqlColumna = 'SELECT DISTINCT d.column_name as Nombre, d.DATA_TYPE as TipoDeDato ,
			IIF (d.CHARACTER_MAXIMUM_LENGTH is not null,
				concat(''('',CONVERT(varchar(10), d.CHARACTER_MAXIMUM_LENGTH),'')''),'''') as Longitud,
			 d.is_nullable as EsNuleable, null as EsIdentity,
			 d.column_default as ColumnaDefault, null as TablaCrearId, t.object_id as TablaModificarId,
			 0 as EsAddColumn, 1 as EsDropColumn, 0 as EsAlterColumn, null, null, null  
			FROM '+@nombre_db_Destino+'.INFORMATION_SCHEMA.COLUMNS d join '+@nombre_db_Origen+'.INFORMATION_SCHEMA.COLUMNS o
			on o.TABLE_NAME = d.TABLE_NAME join '+@nombre_db_Destino+'.sys.tables t on d.TABLE_NAME = t.name
			WHERE d.COLUMN_NAME NOT IN(
				SELECT DISTINCT COLUMN_NAME
				FROM '+@nombre_db_Origen+'.INFORMATION_SCHEMA.COLUMNS)
			order by t.object_id'
			insert into Columna
			exec sp_executesql @sqlColumna

			
			set @sqlColumna = 'select ot.column_name as Nombre, ot.DATA_TYPE as TipoDeDato ,
			IIF (ot.CHARACTER_MAXIMUM_LENGTH is not null,
				concat(''('',CONVERT(varchar(10), ot.CHARACTER_MAXIMUM_LENGTH),'')''),'''') as Longitud,
			ot.is_nullable as EsNuleable, 
				co.is_identity as EsIdentity,
				ot.column_default as ColumnaDefault, null as TablaCrearId, co.object_id as TablaModificarId,
				0 as EsAddColumn, 0 as EsDropColumn, 1 as EsAlterColumn, 
				Referencia = 
				case
					when ot.COLUMN_NAME in (select name from '+@nombre_db_Destino+'.sys.columns i
											join '+@nombre_db_Destino+'.sys.foreign_key_columns k on i.column_id = k.referenced_column_id 
											and i.object_id = k.referenced_object_id) 
										then (select fk.name from '+@nombre_db_Destino+'.sys.columns i
											join '+@nombre_db_Destino+'.sys.foreign_key_columns k on i.column_id = k.referenced_column_id 
											and i.object_id = k.referenced_object_id
											join '+@nombre_db_Destino+'.sys.foreign_keys fk on fk.object_id = k.constraint_object_id)
					else null
					end,
				NombreTablaReferencia =
				case
				when ot.COLUMN_NAME in (select name from '+@nombre_db_Destino+'.sys.columns i
										join '+@nombre_db_Destino+'.sys.foreign_key_columns k on i.column_id = k.referenced_column_id 
										and i.object_id = k.referenced_object_id) 
									then (select t.name from '+@nombre_db_Destino+'.sys.columns i
										join '+@nombre_db_Destino+'.sys.foreign_key_columns k on i.column_id = k.referenced_column_id 
										and i.object_id = k.referenced_object_id
										join '+@nombre_db_Destino+'.sys.foreign_keys fk on fk.object_id = k.constraint_object_id
										join '+@nombre_db_Destino+'.sys.tables t on t.object_id = k.parent_object_id)
				else null
				end,
				Pk =
				   case
					when ot.COLUMN_NAME in (select cu.COLUMN_NAME from '+@nombre_db_Destino+'.sys.key_constraints kc
											join '+@nombre_db_Destino+'.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE cu
											on kc.name = cu.CONSTRAINT_NAME where co.object_id = kc.parent_object_id
											and kc.type = ''PK'') 
										then (select kc.name from '+@nombre_db_Destino+'.sys.key_constraints kc
											join '+@nombre_db_Destino+'.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE cu
											on kc.name = cu.CONSTRAINT_NAME where co.object_id = kc.parent_object_id
											and kc.type = ''PK'')
					else null
					end    
			from '+@nombre_db_Origen+'.sys.columns co join '+@nombre_db_Origen+'.sys.tables ico 
			on co.object_id = ico.object_id 
			join '+@nombre_db_Origen+'.INFORMATION_SCHEMA.COLUMNS ot on ico.name = ot.TABLE_NAME and ot.COLUMN_NAME = co.name
			where exists (select * from '+@nombre_db_Destino+'.sys.columns cd join '+@nombre_db_Destino+'.sys.tables ic 
			on cd.object_id = ic.object_id 
			join '+@nombre_db_Destino+'.INFORMATION_SCHEMA.COLUMNS dt on ic.name = ot.TABLE_NAME 
			and ot.COLUMN_NAME = cd.name
			where ico.object_id = ic.object_id and ot.COLUMN_NAME = dt.COLUMN_NAME
			and (co.is_identity != cd.is_identity or
				(dt.DATA_TYPE != ot.DATA_TYPE or dt.CHARACTER_MAXIMUM_LENGTH != ot.CHARACTER_MAXIMUM_LENGTH
							or dt.IS_NULLABLE != ot.IS_NULLABLE or 
							(dt.COLUMN_DEFAULT != ot.COLUMN_DEFAULT 
								or dt.COLUMN_DEFAULT is not null and ot.COLUMN_DEFAULT is null 
								or dt.COLUMN_DEFAULT is null and ot.COLUMN_DEFAULT is not null)
							)
				)
			)'

			insert into Columna
			exec sp_executesql @sqlColumna
			

			--insertamos las fk de la bdd Origen en nuestra tabla de ClavesForaneas
			set @sqlForanea = 'SELECT 
			st.TABLE_SCHEMA as Esquema, t.name TablaConFk,
			fk.name as NombreFk, cf.name as NombreColumnaFk,
			tt.name TablaReferences,
			c.name as NombreColumnaReferencia
			FROM '+@nombre_db_Origen+'.sys.foreign_keys fk join '+@nombre_db_Origen+'.sys.foreign_key_columns fkc
			on fk.OBJECT_ID = fkc.constraint_object_id join '+@nombre_db_Origen+'.sys.columns c
			on c.OBJECT_ID = fkc.referenced_object_id
				AND c.column_id = fkc.referenced_column_id join '+@nombre_db_Origen+'.sys.columns cf
			on cf.OBJECT_ID = fkc.parent_object_id
				AND cf.column_id = FKC.parent_column_id
			join '+@nombre_db_Origen+'.sys.tables t on fk.parent_object_id = t.object_id 
			join '+@nombre_db_Origen+'.sys.tables tt on fk.referenced_object_id = tt.object_id
			join '+@nombre_db_Origen+'.INFORMATION_SCHEMA.TABLES st on t.name = st.TABLE_NAME  
			where fk.name not in (select fkd.name from '+@nombre_db_Destino+'.sys.foreign_keys fkd)'

			insert into ClaveForaneaCrear
			exec sp_executesql @sqlForanea


			set @sqlForanea = 'SELECT 
			st.TABLE_SCHEMA as Esquema, t.name TablaConFk, fk.name as NombreFk
			FROM '+@nombre_db_Destino+'.sys.foreign_keys fk join '+@nombre_db_Destino+'.sys.foreign_key_columns fkc
			on fk.OBJECT_ID = fkc.constraint_object_id
			join '+@nombre_db_Destino+'.sys.tables t on fk.parent_object_id = t.object_id
			join '+@nombre_db_Destino+'.INFORMATION_SCHEMA.TABLES st on t.name = st.TABLE_NAME  
			where fk.name not in (select fkd.name from '+@nombre_db_Origen+'.sys.foreign_keys fkd)'

			insert into ConstraintEliminar
			exec sp_executesql @sqlForanea

			----otras constraint
			set @sqlOtrasConstraint = 'select tc.CONSTRAINT_SCHEMA, tc.TABLE_NAME, tc.CONSTRAINT_NAME, 
			tc.CONSTRAINT_TYPE, cu.COLUMN_NAME, ch.definition 
			from '+@nombre_db_Origen+'.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
			join '+@nombre_db_Origen+'.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE cu on tc.CONSTRAINT_NAME = cu.CONSTRAINT_NAME
			left join '+@nombre_db_Origen+'.sys.check_constraints 
			ch on tc.CONSTRAINT_TYPE collate SQL_Latin1_General_CP1_CI_AS = left(ch.type_desc, 5) collate SQL_Latin1_General_CP1_CI_AS
			where tc.CONSTRAINT_TYPE != ''FOREIGN KEY'' and tc.CONSTRAINT_NAME not in (select CONSTRAINT_NAME 
			from '+@nombre_db_Destino+'.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE)'

			insert into OtrasConstraintCrear
			exec sp_executesql @sqlOtrasConstraint

			set @sqlOtrasConstraint = 'select tc.CONSTRAINT_SCHEMA, tc.TABLE_NAME, tc.CONSTRAINT_NAME
			from '+@nombre_db_Destino+'.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc 
			where tc.CONSTRAINT_TYPE != ''FOREIGN KEY''
			and tc.CONSTRAINT_NAME not in (select CONSTRAINT_NAME 
				from '+@nombre_db_Origen+'.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE)'

			insert into ConstraintEliminar
			exec sp_executesql @sqlOtrasConstraint
			
			--llamamos a las funciones para crear el script final y llamamos al procedimiento
			--que crea el .sql
			Declare @Path VARCHAR(255)
			Declare @query VARCHAR(max)
			Set @Path='C:\ScriptGeneradosConSql'
			set @query = dbo.CrearTabla()
			set @query += dbo.ModificarTablaAddColumn()
			set @query += dbo.ModificarTablaDropColumn()
			set @query += dbo.ModificarTablaAlterColumn()
			set @query += dbo.AgregarOtrasConstraint()
			set @query += dbo.AgregarFk()
			set @query += dbo.EliminarConstraint()
			set @query += dbo.EliminarTabla()

			execute sp_WriteStringToFile @query, @Path,'prueba.sql'
			commit
		end try
		begin catch
			rollback
			insert into LogErrores
			values(ERROR_PROCEDURE(),ERROR_MESSAGE(),ERROR_LINE())
			print 'Ha ocurrido un error. Puede ver mas detalles en la tabla logErrores'
		end catch
end
go

--ejecucion del procedimiento
use DB_Temporal
go
--exec sp_Compare 'Origen', 'Destino'

----para ver resultados
--select * from BaseDeDatos
--select * from Tabla_Crear
--select * from Tabla_Modificar
--select * from Columna
--select * from Tabla_Eliminar
--select * from ClaveForaneaCrear
--select * from OtrasConstraintCrear
--select * from ConstraintEliminar
--select * from LogErrores


--procedimiento para verificar normas de codificacion
-- ==================================================================================================
-- Autores: Balanda, Sergio
--			Casuscelli, Alejandra
--			Lucero, Nicolás
--			falta una
--			Martin, Florencia	
-- Fecha de Creación: 05/11/2018
-- Descripción:	Stored procedure que verifica las normas de codificación de una base de datos
-- ingresada por parámetro y graba el resultado en una tabla de normas.
-- ==================================================================================================

IF  EXISTS (SELECT * FROM sys.procedures WHERE object_id = OBJECT_ID(N'sp_VerificarNormasCodificacion'))
drop proc sp_VerificarNormasCodificacion
go
create proc sp_VerificarNormasCodificacion @nombreBdd varchar(20)
as
begin
	set nocount on
	delete from	Norma
	begin tran
		begin try
			declare @nombreConstraint varchar(100), @tipoConstraint varchar(20), @nombreTabla varchar(50),
			@nombreProcedure varchar(50), @nombreView varchar(50), @nombreTrigger varchar(50), @evento varchar(20),
			@nombreColumna varchar(50), @tablaOrigen varchar(50), @tablaReferencia varchar(50), 
			@sqlConstraint nvarchar(max),
			@sqlTablas nvarchar(max),
			@sqlColumnas nvarchar(max),
			@sqlProcedures nvarchar(max),
			@sqlViews nvarchar(max),
			@sqlTriggers nvarchar(max)

			set @sqlConstraint= 'declare CursorConstraint cursor for select c.CONSTRAINT_NAME, c.CONSTRAINT_TYPE, c.TABLE_NAME, u.COLUMN_NAME, t.name as TablaOrigen,
				tt.name as TablaReferencia 
				from '+@nombreBdd+'.INFORMATION_SCHEMA.TABLE_CONSTRAINTS c
				left join '+@nombreBdd+'.sys.foreign_keys fk on c.CONSTRAINT_NAME = fk.name
				left join '+@nombreBdd+'.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE u on u.CONSTRAINT_NAME = c.CONSTRAINT_NAME
				left join '+@nombreBdd+'.sys.tables t on fk.parent_object_id = t.object_id 
				left join '+@nombreBdd+'.sys.tables tt on fk.referenced_object_id = tt.object_id'

			set @sqlTablas = 'declare CursorTablas cursor
							  for select t.name from '+@nombreBdd+'.sys.tables t'
			set @sqlColumnas = 'declare CursorColumnas cursor
								for select t.name, c.name as NombreColumna
								from '+@nombreBdd+'.sys.tables t 
								join '+@nombreBdd+'.sys.columns c on t.object_id = c.object_id'
			set @sqlProcedures='declare CursorProcedures cursor
								for select p.name from '+@nombreBdd+'.sys.procedures p'
			set @sqlViews='declare CursorViews cursor
								for select v.name from '+@nombreBdd+'.sys.views v'
			set @sqlTriggers='declare CursorTriggers cursor
								for select t.name, te.type_desc from '+@nombreBdd+'.sys.trigger_events te 
								join '+@nombreBdd+'.sys.triggers t on te.object_id = t.object_id'

			--base de datos que no cumple con las normas
			if(left(@nombreBdd,3) != 'DB_')
				insert into Norma(Nombre, Motivo)
				values(@nombreBdd,'El nombre de la base de datos debe contener el prefijo DB_')
	
			--constraint y campos relacionados que no cumplen con las normas
			exec sp_executesql @sqlConstraint --crea el cursor
			open CursorConstraint
			fetch next from CursorConstraint into @nombreConstraint, @tipoConstraint, @nombreTabla, @nombreColumna,
			@tablaOrigen, @tablaReferencia 
			while @@FETCH_STATUS = 0
			begin
				if(@tipoConstraint = 'PRIMARY KEY')
				begin
					if(left(@nombreConstraint,3) != 'PK_')
						insert into Norma(Origen, Nombre, Motivo)
						values(@nombreTabla, @nombreConstraint,'La clave primaria debe comenzar con el prefijo PK_')
					if(SUBSTRING(@nombreConstraint, CHARINDEX('_', @nombreConstraint) + 1, LEN(@nombreConstraint)) != @nombreTabla)
						insert into Norma(Origen, Nombre, Motivo)
						values(@nombreTabla, @nombreConstraint,'La clave primaria debe contener el nombre de la tabla que la contiene')
					if(@nombreColumna != concat(@nombreTabla,'Id'))
						insert into Norma(Origen, Nombre, Motivo)
						values(@nombreTabla, @nombreColumna,'El nombre de la columna con PK se denominará con el nombre de la tabla seguido de la palabra Id')
				end

				if(@tipoConstraint = 'FOREIGN KEY')
				begin
					if(left(@nombreConstraint,3) != 'FK_')
						insert into Norma(Origen, Nombre, Motivo)
						values(@nombreTabla, @nombreConstraint,'La clave foranea debe comenzar con el prefijo FK_')
					if(SUBSTRING(@nombreConstraint, CHARINDEX('_', @nombreConstraint) + 1, LEN(@nombreConstraint)) != concat(@tablaOrigen, '_', @tablaReferencia))
						insert into Norma(Origen, Nombre, Motivo)
						values(@nombreTabla,@nombreConstraint,'La clave foranea debe contener el nombre de la tabla que la contiene, seguido por el caracter  ''_''  y luego el nombre de la tabla de referencia')
					if(@nombreColumna != concat(@tablaReferencia,'Id'))
						insert into Norma(Origen, Nombre, Motivo)
						values(@nombreTabla, @nombreColumna,'El nombre de la columna que tiene la FK se denominará como la primary key al que referencia')
				end
		
				if(@tipoConstraint = 'CHECK')
				begin
					if(left(@nombreConstraint,3) != 'CK_')
						insert into Norma(Origen, Nombre, Motivo)
						values(@nombreTabla,@nombreConstraint,'Las constraint de tipo Check deben comenzar con el prefijo CK_')
					if(SUBSTRING(@nombreConstraint, CHARINDEX('_', @nombreConstraint) + 1, LEN(@nombreConstraint)) != @nombreColumna)
						insert into Norma(Origen, Nombre, Motivo)
						values(@nombreTabla, @nombreConstraint,'Las constraint de tipo Check deben comenzar con el prefijo CK_
						y contener el nombre de la columna con la constraint')
				
				end

				if(@tipoConstraint = 'UNIQUE')
				begin
					if(left(@nombreConstraint,3) != 'UQ_')
						insert into Norma(Origen, Nombre, Motivo)
						values(@nombreTabla,@nombreConstraint,'Los campos Unique deben comenzar con el prefijo UQ_')
					if(SUBSTRING(@nombreConstraint, CHARINDEX('_', @nombreConstraint) + 1, LEN(@nombreConstraint)) != @nombreColumna)
						insert into Norma(Nombre, Motivo)
						values(@nombreConstraint,'Los campos Unique deben contener el nombre de la columna con la constraint')
				end
				fetch next from CursorConstraint into @nombreConstraint, @tipoConstraint, @nombreTabla, @nombreColumna,
					@tablaOrigen, @tablaReferencia 
			end
			close CursorConstraint
			deallocate CursorConstraint

			--tablas que no cumplen con las normas
			exec sp_executesql @sqlTablas --crea el cursor
			open CursorTablas
			fetch next from CursorTablas into @nombreTabla
			while @@FETCH_STATUS = 0
			begin
				if(left(@nombreTabla,1) COLLATE Latin1_General_CS_AS != upper(left(@nombreTabla,1)))
					insert into Norma(Nombre, Motivo)
					values(@nombreTabla, 'El nombre de la tabla debe comenzar con letra mayúscula.')
				fetch next from CursorTablas into @nombreTabla
			end
			close CursorTablas
			deallocate CursorTablas

			-- columnas que no cumplen con las normas
			exec sp_executesql @sqlColumnas --crea el cursor
			open CursorColumnas
			fetch next from CursorColumnas into @nombreTabla, @nombreColumna
			while @@FETCH_STATUS = 0
			begin
				if(left(@nombreColumna,1) COLLATE Latin1_General_CS_AS != upper(left(@nombreColumna,1)))
					insert into Norma(Origen, Nombre, Motivo)
					values(@nombreTabla, @nombreColumna, 'El nombre de la columna debe comenzar con letra mayúscula.')
				fetch next from CursorColumnas into @nombreTabla, @nombreColumna
			end
			close CursorColumnas
			deallocate CursorColumnas

			--store procedures 
			exec sp_executesql @sqlProcedures
			open CursorProcedures
			fetch next from CursorProcedures into @nombreProcedure
			while @@FETCH_STATUS = 0
				begin
					if(left(@nombreProcedure,3) != 'sp_')
							insert into Norma(Nombre, Motivo)
							values(@nombreProcedure,'El nombre del store procedure debe comenzar con el prefijo sp_')
					fetch next from CursorProcedures into @nombreProcedure
				end
			close CursorProcedures
			deallocate CursorProcedures

			--views
			exec sp_executesql @sqlViews
			open CursorViews
			fetch next from CursorViews into @nombreView
			while @@FETCH_STATUS = 0
				begin
					if(left(@nombreView,2) != 'v_')
							insert into Norma(Nombre, Motivo)
							values(@nombreView,'El nombre de la vista debe comenzar con el prefijo v_')
					fetch next from CursorViews into @nombreView
				end
			close CursorViews
			deallocate CursorViews

			--triggers
			exec sp_executesql @sqlTriggers --crea el cursor
			open CursorTriggers
			fetch next from CursorTriggers into @nombreTrigger, @evento
			while @@FETCH_STATUS = 0
				begin
					if(@evento = 'insert')
					begin
						if(left(@nombreTrigger,4) != 'TGI_')
								insert into Norma(Nombre, Motivo)
								values(@nombreTrigger,'Los triggers para insert deben comenzar con el prefijo TGI_')
					end
					if(@evento = 'update')
					begin
						if(left(@nombreTrigger,4) != 'TGU_')
								insert into Norma(Nombre, Motivo)
								values(@nombreTrigger,'Los triggers para update deben comenzar con el prefijo TGU_')
					end
					if(@evento = 'delete')
					begin
						if(left(@nombreTrigger,4) != 'TGD_')
								insert into Norma(Nombre, Motivo)
								values(@nombreTrigger,'Los triggers para delete deben comenzar con el prefijo TGD_')
					end
					fetch next from CursorTriggers into @nombreTrigger, @evento
				end
			close CursorTriggers
			deallocate CursorTriggers
			commit
		end try
		begin catch
			rollback
			select ERROR_MESSAGE()
		end catch
end
go

--ejecutar procedimiento para evaluar normas de codificacion
--exec sp_VerificarNormasCodificacion 'Origen'

--para ver resultados
--select * from Norma










