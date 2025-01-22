from datetime import datetime, timedelta
from kubernetes.client import models as k8s
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.helpers import chain, cross_downstream
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
import pendulum
local_tz = pendulum.timezone("Asia/Seoul")
import sys
sys.path.append('/opt/bitnami/airflow/dags/git_sa-common')

from icis_common import *
COMMON = ICISCmmn(DOMAIN='oder',ENV='prd-tz', NAMESPACE='t-order'
                , WORKFLOW_NAME='ET_CSNB044B',WORKFLOW_ID='166198a4520c4e52a3eea47177a9fb80', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-ET_CSNB044B-0.4.prd-tz.1.1'
    ,'schedule_interval':'None'
    ,'start_date': datetime(2025, 1, 10, 10, 41, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('166198a4520c4e52a3eea47177a9fb80')

    csnb044bJob_vol = []
    csnb044bJob_volMnt = []
    csnb044bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csnb044bJob_env.extend([getICISConfigMap('icis-oder-etcterr-batch-mng-configmap'), getICISSecret('icis-oder-etcterr-batch-mng-secret'), getICISConfigMap('icis-oder-etcterr-batch-configmap'), getICISSecret('icis-oder-etcterr-batch-secret')])
    csnb044bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnb044bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'a775501ff0804f65962e2551951f2830',
        'volumes': csnb044bJob_vol,
        'volume_mounts': csnb044bJob_volMnt,
        'env_from':csnb044bJob_env,
        'task_id':'csnb044bJob',
        'image':'/icis/icis-oder-etcterr-batch:0.7.1.7',
        'arguments':["--job.name=csnb044bJob", "inputDate=${YYYYMMDD}", "regOfcCd=710327", "regEmpNo=91108904", "date=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('166198a4520c4e52a3eea47177a9fb80')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnb044bJob,
        Complete
    ]) 

    # authCheck >> csnb044bJob >> Complete
    workflow








