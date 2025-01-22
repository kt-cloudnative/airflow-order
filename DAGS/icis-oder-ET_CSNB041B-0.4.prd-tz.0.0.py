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
                , WORKFLOW_NAME='ET_CSNB041B',WORKFLOW_ID='e12c6814c6ed4e54a054f53b215c6334', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-ET_CSNB041B-0.4.prd-tz.0.0'
    ,'schedule_interval':'None'
    ,'start_date': datetime(2025, 1, 10, 10, 17, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('e12c6814c6ed4e54a054f53b215c6334')

    csnb041bJob_vol = []
    csnb041bJob_volMnt = []
    csnb041bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    csnb041bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    csnb041bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csnb041bJob_env.extend([getICISConfigMap('icis-oder-etcterr-batch-mng-configmap'), getICISSecret('icis-oder-etcterr-batch-mng-secret'), getICISConfigMap('icis-oder-etcterr-batch-configmap'), getICISSecret('icis-oder-etcterr-batch-secret')])
    csnb041bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnb041bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '18c41aaeedf24b57ad0010084853fa8e',
        'volumes': csnb041bJob_vol,
        'volume_mounts': csnb041bJob_volMnt,
        'env_from':csnb041bJob_env,
        'task_id':'csnb041bJob',
        'image':'/icis/icis-oder-etcterr-batch:0.4.1.65',
        'arguments':["--job.name=csnb041bJob", "date=${YYYYMMDD}", "flag=A", "np=1", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('e12c6814c6ed4e54a054f53b215c6334')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnb041bJob,
        Complete
    ]) 

    # authCheck >> csnb041bJob >> Complete
    workflow








