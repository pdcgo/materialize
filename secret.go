package materialize

import "gorm.io/gorm"

type SecretManage interface {
	CreateSecret(name string, value string) error
	GetSecret(name string) (string, error)
	UpdateSecret(name string, value string) error
	DeleteSecret(name string) error
	ListSecrets() ([]string, error)
}

type secretManageImpl struct {
	db *gorm.DB
}

type Secret struct {
	ID    uint   `gorm:"primaryKey"`
	Name  string `gorm:"uniqueIndex"`
	Value string
}

// CreateSecret inserts a new secret into the database and also executes the Materialize-specific SQL command
func (s *secretManageImpl) CreateSecret(name string, value string) error {
	// Start a transaction
	return s.db.Transaction(func(tx *gorm.DB) error {
		// Execute the Materialize-specific SQL command to add the secret
		sql := "CREATE SECRET IF NOT EXISTS " + name + " AS '" + value + "'"
		if err := tx.Exec(sql).Error; err != nil {
			return err
		}

		return nil
	})
}
func (s *secretManageImpl) GetSecret(name string) (string, error) {
	var secret Secret
	if err := s.db.Where("name = ?", name).First(&secret).Error; err != nil {
		return "", err
	}
	return secret.Value, nil
}

func (s *secretManageImpl) UpdateSecret(name string, value string) error {
	return s.db.Transaction(func(tx *gorm.DB) error {
		// Update the secret in the secrets table
		if err := tx.Model(&Secret{}).Where("name = ?", name).Update("value", value).Error; err != nil {
			return err
		}
		// Update the Materialize secret
		sql := "ALTER SECRET " + name + " AS '" + value + "'"
		if err := tx.Exec(sql).Error; err != nil {
			return err
		}
		return nil
	})
}

func (s *secretManageImpl) DeleteSecret(name string) error {
	return s.db.Transaction(func(tx *gorm.DB) error {
		// Delete from the secrets table
		if err := tx.Where("name = ?", name).Delete(&Secret{}).Error; err != nil {
			return err
		}
		// Drop the Materialize secret
		sql := "DROP SECRET IF EXISTS " + name
		if err := tx.Exec(sql).Error; err != nil {
			return err
		}
		return nil
	})
}

func (s *secretManageImpl) ListSecrets() ([]string, error) {
	var secrets []Secret
	if err := s.db.Find(&secrets).Error; err != nil {
		return nil, err
	}
	names := make([]string, len(secrets))
	for i, secret := range secrets {
		names[i] = secret.Name
	}
	return names, nil
}

func NewSecretManage(db *gorm.DB) SecretManage {
	return &secretManageImpl{db: db}
}
