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
                , WORKFLOW_NAME='ET_CSNB049B',WORKFLOW_ID='7e9397bbb9cd43b48ca709b3c555f26c', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-ET_CSNB049B-0.4.prd-tz.1.0'
    ,'schedule_interval':'None'
    ,'start_date': datetime(2025, 1, 10, 10, 40, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('7e9397bbb9cd43b48ca709b3c555f26c')

    csnb049bJob_vol = []
    csnb049bJob_volMnt = []
    csnb049bJob_vol.append(getVolume('t-order-ftp-pvc','t-order-ftp-pvc'))
    csnb049bJob_volMnt.append(getVolumeMount('t-order-ftp-pvc','/app/order'))

    csnb049bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    csnb049bJob_env.extend([getICISConfigMap('icis-oder-etcterr-batch-mng-configmap'), getICISSecret('icis-oder-etcterr-batch-mng-secret'), getICISConfigMap('icis-oder-etcterr-batch-configmap'), getICISSecret('icis-oder-etcterr-batch-secret')])
    csnb049bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csnb049bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '1fd792d573194f6dad4bb621fdd0db2e',
        'volumes': csnb049bJob_vol,
        'volume_mounts': csnb049bJob_volMnt,
        'env_from':csnb049bJob_env,
        'task_id':'csnb049bJob',
        'image':'/icis/icis-oder-etcterr-batch:0.7.1.7',
        'arguments':["--job.name=csnb049bJob", "workYm=${YYYYMM}", "workGb=R", "regEmpNo=91108904", "date=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('7e9397bbb9cd43b48ca709b3c555f26c')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csnb049bJob,
        Complete
    ]) 

    # authCheck >> csnb049bJob >> Complete
    workflow








